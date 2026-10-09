"""Tests de ``quarantine_service`` : la quarantaine est un workspace Schematics
séparé du bucket (zone + règle CBR seulement), créé à la pose, détruit à la levée.
Sa zone laisse passer Schematics du compte hub, d'où l'orchestrateur sonde le bucket."""
import sys
from unittest.mock import MagicMock

import pytest

from cos_service.services import quarantine_service as svc

BUCKET = {
    "subscription_id": "sub-1",
    "name": "bucket-a",
    "region": "eu-de",
    "object_versioning_enabled": False,
    "cos": {"crn": "crn:v1:bluemix:public:cloud-object-storage:global:a/acc:guid-cos::", "name": "cos-a",
            "context": {"realm": "realm-a", "app_code": "AP1"}},
}
SECRETS = {"vault_read_addr": "https://vault", "vault_read_token": "rt", "gitlab_token": "gl"}
HUB = "e" * 32
WKL = "a" * 32
# Forme du modèle du reader (model_dump) : *_account_id l'identifiant IBM, *_account_number le numéro.
REALM = {"name": "realm-a", "buhub_account_id": HUB, "wklapp_account_id": WKL, "wklapp_account_number": "2763730"}


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
    mocks["contextService"].get_realm.return_value.model_dump.return_value = REALM
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
    assert svc.quarantine_settings() == {"enforcement_mode": "report", "probe_endpoint": ""}


def test_settings_default_to_enforcement_enabled(deps):
    deps["schematics_service"].setting.side_effect = lambda name, default: default

    assert svc.quarantine_settings() == {"enforcement_mode": "enabled", "probe_endpoint": ""}


class TestRealmAccounts:
    def test_hub_and_workload_ids_come_from_the_reader_model(self, deps):
        variables = svc.quarantine_variables(BUCKET, SECRETS, REALM)

        assert (variables["hub_account_id"], variables["cbr_account_id"]) == (HUB, WKL)

    @pytest.mark.parametrize("missing", ["buhub_account_id", "wklapp_account_id"])
    def test_a_missing_account_refuses_the_quarantine_before_schematics(self, deps, missing):
        realm = {k: v for k, v in REALM.items() if k != missing}
        deps["contextService"].get_realm.return_value.model_dump.return_value = realm

        with pytest.raises(ValueError, match=missing):
            svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=MagicMock(), vault="vault", reader="reader")
        svc.create_or_update_ws.assert_not_called()


class TestProbeEndpoint:
    VIP = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud/bucket-a"

    def test_defaults_to_the_endpoint_of_the_bucket(self, deps):
        variables = svc.quarantine_variables({**BUCKET, "virtual_server_endpoint": self.VIP}, SECRETS, REALM)

        assert variables["probe_endpoint"] == self.VIP

    def test_the_setting_overrides_the_endpoint_of_the_bucket(self, deps):
        deps["schematics_service"].setting.side_effect = lambda name, default: {
            "cos_quarantine_probe_endpoint": "https://override/bucket-a",
        }.get(name, default)

        variables = svc.quarantine_variables({**BUCKET, "virtual_server_endpoint": self.VIP}, SECRETS, REALM)

        assert variables["probe_endpoint"] == "https://override/bucket-a"

    def test_without_either_the_module_builds_its_default(self, deps):
        assert svc.quarantine_variables(BUCKET, SECRETS, REALM)["probe_endpoint"] == ""


class TestProbeViaSchematics:
    def run(self, deps, outputs):
        svc.run_workspace.return_value = outputs
        return svc.probe_bucket_via_schematics(tf="tf", bucket=BUCKET, workspace_id="ws-cbr-1", vault="vault", reader="reader")

    def test_enables_the_probe_reapplies_and_reads_the_outputs(self, deps):
        result = self.run(deps, {"probe_status_code": {"value": 200}, "bucket_empty": {"value": True}})

        assert result == {"status": 200, "empty": True}
        update = deps["schematics_service"].update_ws_variables
        update.assert_called_once()
        assert update.call_args.args[:2] == ("tf", "ws-cbr-1")
        variables = update.call_args.args[2]
        assert variables["probe_enabled"] == "true"
        assert variables["bucket_name"] == "bucket-a" and variables["hub_account_id"] == HUB  # jeu complet
        svc.run_workspace.assert_called_once_with("tf", "ws-cbr-1")

    def test_schematics_blocked_gives_the_status_and_no_emptiness(self, deps):
        result = self.run(deps, {"probe_status_code": {"value": "403"}, "bucket_empty": {"value": None}})

        assert result == {"status": 403, "empty": None}

    def test_missing_outputs_give_none(self, deps):
        assert self.run(deps, {})["status"] is None


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
