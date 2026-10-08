"""Tests de ``quarantine_service`` : la quarantaine est un workspace Schematics
séparé du bucket (zone + règle CBR seulement), créé à la pose, détruit à la levée."""
import sys
from unittest.mock import MagicMock

import pytest

from cos_service.services import quarantine_service as svc

BUCKET = {
    "subscription_id": "sub-1",
    "name": "bucket-a",
    "region": "eu-de",
    "cos": {"crn": "crn:v1:bluemix:public:cloud-object-storage:global:a/acc:guid-cos::", "name": "cos-a",
            "context": {"realm": "realm-a", "app_code": "AP1"}},
}
SECRETS = {"vault_read_addr": "https://vault", "vault_read_token": "rt", "gitlab_token": "gl"}


class SchematicsError(Exception):
    def __init__(self, code):
        super().__init__(f"schematics {code}")
        self.code = code


@pytest.fixture
def deps(monkeypatch):
    mocks = {}
    for name in ("vault_service", "contextService", "schematics_service"):
        mocks[name] = MagicMock(name=name)
        monkeypatch.setitem(sys.modules, f"cos_service.services.{name}", mocks[name])
    mocks["vault_service"].get_vault_secrets.return_value = SECRETS
    mocks["contextService"].get_realm.return_value.model_dump.return_value = {"name": "realm-a", "wklapp_account_number": "wk-123"}
    mocks["schematics_service"].setting.side_effect = lambda name, default: {
        "cos_quarantine_enforcement_mode": "report",
    }.get(name, default)
    # create_or_update_ws / run_workspace sont importés au chargement du module : patchés sur lui.
    monkeypatch.setattr(svc, "create_or_update_ws", MagicMock(return_value={"id": "ws-cbr-1"}))
    monkeypatch.setattr(svc, "run_workspace", MagicMock(return_value={"cbr_rule_id": {"value": "rule-1"}}))
    monkeypatch.setattr(svc, "ENVIRONMENT", "int")
    return mocks


def test_workspace_name_is_derived_from_the_subscription():
    assert svc.quarantine_workspace_name("sub-1") == "ws_cbr_bucket_sub-1"


def test_settings_come_from_airflow_variables_or_env(deps):
    assert svc.quarantine_settings() == {"enforcement_mode": "report"}


def test_settings_default_to_enforcement_enabled(deps):
    deps["schematics_service"].setting.side_effect = lambda name, default: default

    assert svc.quarantine_settings() == {"enforcement_mode": "enabled"}


def test_set_creates_the_separate_workspace_from_database_values_and_applies(deps):
    payload = MagicMock(product_branch="feature/x")

    assert svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=payload, vault="vault", reader="reader") == "ws-cbr-1"

    deps["vault_service"].get_vault_secrets.assert_called_once_with(realm_name="realm-a", apcode="AP1", vault="vault", reader="reader")
    deps["contextService"].get_realm.assert_called_once_with("reader")
    call = svc.create_or_update_ws.call_args
    assert call.args == ("tf",)
    assert call.kwargs["workspace_name"] == "ws_cbr_bucket_sub-1"
    assert call.kwargs["tf_directory"] == "terraform/v1.12/bucket_quarantine"
    assert call.kwargs["orchestrator_env"] == "int"
    assert call.kwargs["gitlab_token"] == "gl"
    assert call.kwargs["product_branch"] == "feature/x"
    variables = call.kwargs["variables"]
    assert variables["bucket_name"] == "bucket-a"  # depuis la base, jamais lu sur COS
    assert variables["cos_instance_crn"] == BUCKET["cos"]["crn"]
    assert variables["wklapp_account_id"] == "wk-123"
    assert variables["app_code"] == "AP1"
    assert "allowed_vpc_crns" not in variables  # la règle bloque tout, aucune zone à tailler
    assert variables["enforcement_mode"] == "report"
    assert variables["vault_read_token"].value == "rt" and variables["vault_read_token"].sensitive is True
    svc.run_workspace.assert_called_once_with("tf", "ws-cbr-1")


def test_set_propagates_an_apply_failure(deps):
    svc.run_workspace.side_effect = RuntimeError("apply failed")

    with pytest.raises(RuntimeError, match="apply failed"):
        svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=MagicMock(), vault="vault", reader="reader")


class TestLift:
    def test_destroys_the_resources_then_deletes_the_workspace(self):
        tf = MagicMock(name="tf")

        svc.lift_bucket_quarantine(tf=tf, workspace_id="ws-cbr-1")

        call = tf.workspaces.delete_workspace_resources.call_args
        assert call.args == ("ws-cbr-1",)
        assert (call.kwargs["cooldown_policy"].delay, call.kwargs["cooldown_policy"].max_attempts) == (5, 120)
        tf.workspaces.get_by_id.return_value.delete.assert_called_once()

    def test_workspace_already_gone_counts_as_lifted(self):
        tf = MagicMock(name="tf")
        tf.workspaces.get_by_id.side_effect = SchematicsError(404)

        svc.lift_bucket_quarantine(tf=tf, workspace_id="ws-cbr-1")

        tf.workspaces.delete_workspace_resources.assert_not_called()

    def test_other_error_propagates(self):
        tf = MagicMock(name="tf")
        tf.workspaces.delete_workspace_resources.side_effect = SchematicsError(500)

        with pytest.raises(SchematicsError):
            svc.lift_bucket_quarantine(tf=tf, workspace_id="ws-cbr-1")
