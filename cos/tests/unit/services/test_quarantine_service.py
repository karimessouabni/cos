"""Tests de ``quarantine_service`` : la quarantaine est un workspace Schematics
séparé du bucket (zone + règle CBR seulement). La règle bloque tout ;
l'orchestrateur l'ouvre (règle désactivée) le temps d'agir, puis la referme.
Le workspace est créé à la pose et détruit à la levée (ADR 0004)."""
import sys
from unittest.mock import MagicMock, call

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
WKL = "a" * 32
# Forme du modèle du reader (model_dump) : *_account_id l'identifiant IBM, *_account_number le numéro.
REALM = {"name": "realm-a", "wklapp_account_id": WKL, "wklapp_account_number": "2763730"}


class SchematicsError(Exception):
    def __init__(self, code):
        super().__init__(f"schematics {code}")
        self.code = code


@pytest.fixture
def deps(monkeypatch):
    mocks = {}
    for name in ("vault_service", "contextService", "schematics_service", "bucketService"):
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
    assert svc.quarantine_settings() == {"enforcement_mode": "report"}


def test_settings_default_to_enforcement_enabled(deps):
    deps["schematics_service"].setting.side_effect = lambda name, default: default

    assert svc.quarantine_settings() == {"enforcement_mode": "enabled"}


class TestVariables:
    def test_from_the_database_and_the_realm_rule_active_by_default(self, deps):
        variables = svc.quarantine_variables(BUCKET, SECRETS, REALM)

        assert variables["bucket_name"] == "bucket-a"  # depuis la base, jamais lu sur COS
        assert variables["cos_instance_crn"] == BUCKET["cos"]["crn"]
        assert variables["wklapp_account_id"] == "2763730"  # chemin Vault, comme le module bucket
        assert variables["cbr_account_id"] == WKL  # propriétaire de la zone et de la règle
        assert variables["rule_active"] == "true"  # Schematics reçoit des chaînes
        assert variables["enforcement_mode"] == "report"
        assert variables["vault_read_token"].value == "rt" and variables["vault_read_token"].sensitive is True
        # Plus de compte hub ni de sonde : aucune zone ne laisse passer l'orchestrateur.
        assert not {"hub_account_id", "probe_enabled", "probe_endpoint"} & set(variables)

    def test_an_open_quarantine_disables_the_rule(self, deps):
        assert svc.quarantine_variables(BUCKET, SECRETS, REALM, active=False)["rule_active"] == "false"

    def test_without_the_workload_account_id_no_quarantine(self, deps):
        deps["contextService"].get_realm.return_value.model_dump.return_value = {"name": "realm-a"}

        with pytest.raises(ValueError, match="wklapp_account_id"):
            svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=MagicMock(), vault="vault", reader="reader")
        svc.create_or_update_ws.assert_not_called()  # refusé avant tout appel Schematics


class TestSet:
    def test_creates_the_separate_workspace_rewrites_its_variables_and_applies(self, deps):
        payload = MagicMock(product_branch="feature/x")

        assert svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=payload, vault="vault", reader="reader") == "ws-cbr-1"

        deps["vault_service"].get_vault_secrets.assert_called_once_with(realm_name="realm-a", apcode="AP1", vault="vault", reader="reader")
        created = svc.create_or_update_ws.call_args
        assert created.args == ("tf",)
        assert created.kwargs["workspace_name"] == "ws_cbr_bucket_sub-1"
        assert created.kwargs["tf_directory"] == "terraform/v1.12/bucket_quarantine"
        assert created.kwargs["product_branch"] == "feature/x"
        variables = created.kwargs["variables"]
        assert variables["rule_active"] == "true"
        # le workspace peut préexister (run précédent) : ses variables sont réécrites en entier
        deps["schematics_service"].update_ws_variables.assert_called_once_with("tf", "ws-cbr-1", variables)
        svc.run_workspace.assert_called_once_with("tf", "ws-cbr-1")

    def test_propagates_an_apply_failure(self, deps):
        svc.run_workspace.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            svc.set_bucket_quarantine(tf="tf", bucket=BUCKET, payload=MagicMock(), vault="vault", reader="reader")


class TestProbeUntil:
    def test_stops_at_the_expected_status_and_reports_the_delay(self, deps):
        deps["bucketService"].bucket_access_status.side_effect = [403, 403, 200]
        sleep = MagicMock()

        result = svc.probe_bucket_until("tok", BUCKET, 200, sleep=sleep, interval=30, attempts=5)

        assert result == {"reached": True, "status": 200, "attempts": 3, "seconds": 60}
        assert sleep.call_count == 2
        deps["bucketService"].bucket_access_status.assert_called_with("tok", BUCKET)

    def test_gives_up_after_the_attempts_without_raising(self, deps):
        deps["bucketService"].bucket_access_status.side_effect = [403, 500, 403]
        sleep = MagicMock()

        result = svc.probe_bucket_until("tok", BUCKET, 200, sleep=sleep, interval=30, attempts=3)

        assert result == {"reached": False, "status": 403, "attempts": 3, "seconds": 60, "statuses_seen": [403, 500]}
        assert sleep.call_count == 2  # pas d'attente après le dernier essai

    def test_defaults_bound_the_wait_to_twenty_minutes(self):
        assert svc.PROBE_INTERVAL_SECONDS * (svc.PROBE_MAX_ATTEMPTS - 1) == 19.5 * 60


class TestOpenAndClose:
    def open(self):
        return svc.open_bucket_quarantine(
            tf="tf", bucket=BUCKET, workspace_id="ws-cbr-1", vault="vault", reader="reader", access_token="tok"
        )

    def applied_rule_states(self, deps) -> list:
        update = deps["schematics_service"].update_ws_variables
        return [c.args[2]["rule_active"] for c in update.call_args_list]

    def test_open_disables_the_rule_then_waits_for_the_access(self, deps, monkeypatch):
        probe = MagicMock(return_value={"reached": True, "status": 200, "attempts": 4, "seconds": 90})
        monkeypatch.setattr(svc, "probe_bucket_until", probe)

        assert self.open() == {"reached": True, "status": 200, "attempts": 4, "seconds": 90}

        assert self.applied_rule_states(deps) == ["false"]
        assert deps["schematics_service"].update_ws_variables.call_args.args[:2] == ("tf", "ws-cbr-1")
        svc.run_workspace.assert_called_once_with("tf", "ws-cbr-1")
        probe.assert_called_once_with("tok", BUCKET, 200)
        deps["vault_service"].get_vault_secrets.assert_called_once()  # tokens Vault frais à chaque apply

    def test_access_not_back_closes_the_quarantine_again_and_raises(self, deps, monkeypatch):
        """Agir sur un bucket encore bloqué lirait un 403 comme une réponse :
        le bucket reste du côté sûr, fermé."""
        monkeypatch.setattr(svc, "probe_bucket_until", MagicMock(
            return_value={"reached": False, "status": 403, "attempts": 40, "seconds": 1170}
        ))

        with pytest.raises(RuntimeError, match="still answers HTTP 403 1170 s after.*rule is active again"):
            self.open()

        assert self.applied_rule_states(deps) == ["false", "true"]
        assert svc.run_workspace.call_args_list == [call("tf", "ws-cbr-1"), call("tf", "ws-cbr-1")]

    def test_close_enables_the_rule_again(self, deps):
        svc.close_bucket_quarantine(tf="tf", bucket=BUCKET, workspace_id="ws-cbr-1", vault="vault", reader="reader")

        assert self.applied_rule_states(deps) == ["true"]
        svc.run_workspace.assert_called_once_with("tf", "ws-cbr-1")
        deps["bucketService"].bucket_access_status.assert_not_called()  # pas d'attente à la refermeture

    def test_an_apply_failure_propagates(self, deps):
        svc.run_workspace.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            svc.close_bucket_quarantine(tf="tf", bucket=BUCKET, workspace_id="ws-cbr-1", vault="vault", reader="reader")


class TestLift:
    def test_destroys_the_resources_then_deletes_the_workspace(self):
        tf = MagicMock(name="tf")

        svc.lift_bucket_quarantine(tf=tf, workspace_id="ws-cbr-1")

        destroyed = tf.workspaces.delete_workspace_resources.call_args
        assert destroyed.args == ("ws-cbr-1",)
        assert (destroyed.kwargs["cooldown_policy"].delay, destroyed.kwargs["cooldown_policy"].max_attempts) == (5, 120)
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
