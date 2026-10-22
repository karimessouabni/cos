"""Tests des étapes du DAG ``cos.backup_vault.v1.create``.

Chaque étape est appelée comme une fonction avec ses dépendances passées
explicitement ; les modules ``cos_service.services.*`` sont des ``MagicMock``
(fixture ``services``).
"""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from bp2i_terraform.backends.schematics import TerraformVar
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus
from tests.unit.dags.support import ACCOUNT_CRNS, COS_INSTANCE, REALM, REALM_DICT, SECRETS, FakeModel

TF_OUTPUTS = {
    "backup_vault_name": {"value": "bv-a"},
    "backup_vault_crn": {"value": "crn:bv"},
    "backup_vault_id": {"value": "bv-id"},
}


@pytest.fixture
def vault_dag(load_dag):
    # Chemin relatif à dags/bucket/v1 : fonctionne aussi avec un conftest qui ne
    # cherche les DAGs que sous bucket/v1.
    return load_dag("../../backup_vault/v1/cos.backup_vault.v1.create.py")


@pytest.fixture
def payload(vault_dag):
    return vault_dag.module.CosBackupVaultCreatePayload(
        realm="realm-a", apcode="AP1", environment="dev", region="eu-de", subscription_id="sub-1",
        cos_instance="cos-a",
    )


@pytest.fixture
def ready(services):
    """Mocks d'une demande valide."""
    services.contextService.get_realm.return_value = REALM
    services.contextService.get_apcodes.return_value = ["AP1"]
    services.contextService.get_account_instances_crn.return_value = ACCOUNT_CRNS
    services.cosService.get_cos_instance_by_name.return_value = COS_INSTANCE
    services.cosService.get_cos_instance_status.return_value = SubscriptionStatus.ACTIVE.value
    services.vault_service.get_vault_secrets.return_value = SECRETS
    services.backup_vault_service.get_backup_vault_by_sub_id.return_value = None
    services.backup_vault_service.process_backup_vault_creation.return_value = {"workspace": {"workspace_id": None}}
    services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}
    return services


@pytest.fixture
def tf():
    tf = MagicMock(name="tf")
    workspace = tf.workspaces.get_by_id.return_value
    workspace.apply.return_value.get_logs.return_value = {"tpl-1": "logs"}
    workspace.get_outputs.return_value = [MagicMock(output_values=[TF_OUTPUTS])]
    return tf


def test_dag_declares_the_expected_steps_in_order(vault_dag):
    assert vault_dag.module.backup_vault_create.dag_name == "cos.backup_vault.v1.create"
    assert list(vault_dag.steps) == [
        "validate_context_realm_apcode",
        "get_account_instances_crn",
        "create_tf_workspace",
        "apply_tf_workspace",
        "save_backup_vault_in_db",
    ]


def test_payload_default_cos_instance(vault_dag):
    assert vault_dag.module.CosBackupVaultCreatePayload(subscription_id="s").cos_instance == "co21000001"


class TestValidateContextRealmApcode:
    def run(self, vault_dag, payload, state_manager):
        return vault_dag.steps["validate_context_realm_apcode"](
            payload=payload, reader="reader", session="session", state_manager=state_manager
        )

    def errors_of(self, vault_dag, payload, state_manager) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(vault_dag, payload, state_manager)
        return str(excinfo.value).split(" | ")

    def test_valid_request_returns_the_cos_instance(self, vault_dag, ready, payload, state_manager):
        assert self.run(vault_dag, payload, state_manager) == dict(COS_INSTANCE)

        ready.contextService.get_realm.assert_called_once_with("reader")
        ready.cosService.get_cos_instance_by_name.assert_called_once_with("cos-a", "session")
        ready.cosService.get_cos_instance_status.assert_called_once_with("cos-a", "reader")
        state_manager.push_state.assert_called_once_with({"cos_instance": "cos-a"})

    def test_unknown_realm(self, vault_dag, ready, payload, state_manager):
        ready.contextService.get_realm.return_value = None

        assert self.errors_of(vault_dag, payload, state_manager) == ["the realm realm-a doesn't exist or is empty"]

    def test_empty_realm_in_payload(self, vault_dag, ready, payload, state_manager):
        payload.realm = ""

        assert self.errors_of(vault_dag, payload, state_manager) == ["the realm  doesn't exist or is empty"]

    def test_realm_404(self, vault_dag, ready, payload, state_manager):
        ready.contextService.get_realm.return_value = FakeModel(**{**REALM_DICT, "status": "404"})

        assert "the realm realm-a is not found" in self.errors_of(vault_dag, payload, state_manager)

    def test_realm_without_apcodes(self, vault_dag, ready, payload, state_manager):
        ready.contextService.get_realm.return_value = FakeModel(**{**REALM_DICT, "realm_apcode_details": None})

        assert self.errors_of(vault_dag, payload, state_manager) == ["there is no apcodes on this realm realm-a"]
        ready.contextService.get_apcodes.assert_not_called()

    def test_apcode_not_in_realm(self, vault_dag, ready, payload, state_manager):
        ready.contextService.get_apcodes.return_value = ["AP9"]

        assert self.errors_of(vault_dag, payload, state_manager) == ["appCode AP1 doesn't belong to this realm realm-a"]

    def test_missing_cos_instance_is_declined_without_crashing(self, vault_dag, ready, payload, state_manager):
        """L'original lisait ``cos_instance.name`` sur None : AttributeError au lieu du refus."""
        ready.cosService.get_cos_instance_by_name.return_value = None

        assert self.errors_of(vault_dag, payload, state_manager) == ["the cos instance cos-a doesn't exist"]
        ready.cosService.get_cos_instance_status.assert_not_called()

    def test_inactive_cos_instance(self, vault_dag, ready, payload, state_manager):
        ready.cosService.get_cos_instance_status.return_value = "creating"

        assert self.errors_of(vault_dag, payload, state_manager) == ["Bad cos instance status : creating"]

    def test_all_errors_are_reported_at_once(self, vault_dag, ready, payload, state_manager):
        ready.contextService.get_apcodes.return_value = ["AP9"]
        ready.cosService.get_cos_instance_by_name.return_value = None

        assert self.errors_of(vault_dag, payload, state_manager) == [
            "appCode AP1 doesn't belong to this realm realm-a",
            "the cos instance cos-a doesn't exist",
        ]
        # Le nom d'instance est poussé dans le state même quand la demande est refusée.
        state_manager.push_state.assert_called_once_with({"cos_instance": "cos-a"})


def test_get_account_instances_crn_goes_through_the_reader(vault_dag, ready):
    assert vault_dag.steps["get_account_instances_crn"](cos_instance=dict(COS_INSTANCE), reader="reader") == ACCOUNT_CRNS
    ready.contextService.get_account_instances_crn.assert_called_once_with("reader")


class TestCreateTfWorkspace:
    def run(self, vault_dag, payload, state_manager, tf):
        return vault_dag.steps["create_tf_workspace"](
            account_instances_crn=ACCOUNT_CRNS, tf=tf, state_manager=state_manager, session="session",
            reader="reader", payload=payload, vault="vault",
        )

    def test_creates_the_vault_row_then_the_workspace(self, vault_dag, ready, payload, state_manager, tf):
        assert self.run(vault_dag, payload, state_manager, tf) == "ws-1"

        bv = ready.backup_vault_service
        bv.get_backup_vault_by_sub_id.assert_called_once_with("sub-1", "session")
        bv.process_backup_vault_creation.assert_called_once_with(payload, ACCOUNT_CRNS, "my bucket", COS_INSTANCE, "session")
        args = ready.schematics_service.create_or_update_ws.call_args.args
        assert args[0] is tf
        assert args[1:4] == ("ws_backup_vault_sub-1", "int", "terraform/v1.12/backup_vault")
        assert args[5:] == ("my bucket", "gl")
        bv.update_backup_vault_status.assert_called_once_with("sub-1", SubscriptionStatus.CREATING, "session")
        ready.workspaceService.update_backup_vault_workspace_details.assert_called_once_with(
            {"workspace": {"workspace_id": None}}, {"id": "ws-1"}, "session"
        )
        state_manager.push_state.assert_called_once_with({"workspace_name": "ws_backup_vault_sub-1"})

    def test_terraform_variables(self, vault_dag, ready, payload, state_manager, tf):
        self.run(vault_dag, payload, state_manager, tf)

        variables = ready.schematics_service.create_or_update_ws.call_args.args[4]
        token = variables.pop("vault_read_token")
        assert isinstance(token, TerraformVar) and (token.value, token.sensitive) == ("rt", True)
        assert variables == {
            "region": "eu-fr2",
            "cos_instance_id": "crn:cos",
            "cos_instance_name": "cos-a",
            "kms_key_crn": "crn:kms",
            "vault_read_addr": "ra",
            "app_code": "AP1",
            "orchestrator_environment": "int",
            "wklapp_account_id": "wk-123",
        }
        ready.vault_service.get_vault_secrets.assert_called_once_with(
            realm_name="realm-a", apcode="AP1", vault="vault", reader="reader"
        )

    def test_existing_vault_row_is_not_recreated(self, vault_dag, ready, payload, state_manager, tf):
        ready.backup_vault_service.get_backup_vault_by_sub_id.return_value = {"workspace": {"workspace_id": None}}

        assert self.run(vault_dag, payload, state_manager, tf) == "ws-1"
        ready.backup_vault_service.process_backup_vault_creation.assert_not_called()

    def test_existing_workspace_is_reused(self, vault_dag, ready, payload, state_manager, tf):
        ready.backup_vault_service.get_backup_vault_by_sub_id.return_value = {"workspace": {"workspace_id": "ws-old"}}

        assert self.run(vault_dag, payload, state_manager, tf) == "ws-old"
        ready.schematics_service.create_or_update_ws.assert_not_called()
        ready.backup_vault_service.update_backup_vault_status.assert_not_called()
        state_manager.push_state.assert_not_called()

    def test_failed_workspace_creation_locks_the_vault_and_reraises(self, vault_dag, ready, payload, state_manager, tf):
        """L'original posait FAILED via update_backup_vault_workspace_details (mauvaise fonction)."""
        ready.schematics_service.create_or_update_ws.side_effect = RuntimeError("schematics down")

        with pytest.raises(RuntimeError, match="schematics down"):
            self.run(vault_dag, payload, state_manager, tf)

        ready.backup_vault_service.update_backup_vault_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.LOCKED, "session"
        )
        ready.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
            "sub-1", Status.FAILED, "session"
        )
        ready.workspaceService.update_backup_vault_workspace_details.assert_not_called()


class TestApplyTfWorkspace:
    def run(self, vault_dag, payload, state_manager, tf):
        return vault_dag.steps["apply_tf_workspace"](
            workspace_id="ws-1", account_instances_crn=ACCOUNT_CRNS, tf=tf, state_manager=state_manager,
            reader="reader", session="session", payload=payload, vault="vault",
        )

    def test_updates_the_workspace_then_plans_and_applies(self, vault_dag, ready, payload, state_manager, tf):
        assert self.run(vault_dag, payload, state_manager, tf) == TF_OUTPUTS

        ready.workspaceService.build_backup_vault_workspace_details.assert_called_once_with(
            workspace_id="ws-1", realm="realm-a", app_code="AP1", region="eu-fr2",
            cos_instance_crn="crn:cos", cos_instance_id="crn:cos", cos_instance_name="cos-a",
            kms_crn="crn:kms", description="my bucket",
        )
        ready.backup_vault_service.update_backup_vault_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.CREATING, "session"
        )
        ready.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
            "sub-1", Status.INPROGRESS, "session"
        )
        ready.workspaceService.update_backup_vault_workspace.assert_called_once_with(
            workspace_details=ready.workspaceService.build_backup_vault_workspace_details.return_value,
            vault="vault", tf=tf, reader="reader",
        )
        tf.workspaces.get_by_id.assert_called_once_with("ws-1")
        workspace = tf.workspaces.get_by_id.return_value
        workspace.plan.assert_called_once_with()
        workspace.apply.assert_called_once_with()

    def test_failed_apply_locks_the_vault_and_reraises(self, vault_dag, ready, payload, state_manager, tf):
        tf.workspaces.get_by_id.return_value.apply.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(vault_dag, payload, state_manager, tf)

        ready.backup_vault_service.update_backup_vault_status.assert_called_with("sub-1", SubscriptionStatus.LOCKED, "session")
        ready.workspaceService.update_backup_vault_workspace_status.assert_called_with("sub-1", Status.FAILED, "session")

    def test_failed_workspace_refresh_also_locks_the_vault(self, vault_dag, ready, payload, state_manager, tf):
        ready.workspaceService.update_backup_vault_workspace.side_effect = RuntimeError("refresh failed")

        with pytest.raises(RuntimeError, match="refresh failed"):
            self.run(vault_dag, payload, state_manager, tf)

        tf.workspaces.get_by_id.assert_not_called()
        ready.backup_vault_service.update_backup_vault_status.assert_called_with("sub-1", SubscriptionStatus.LOCKED, "session")


def test_save_backup_vault_in_db_pushes_the_state_and_completes(vault_dag, ready, payload, state_manager):
    result = vault_dag.steps["save_backup_vault_in_db"](
        apply_tf_result=TF_OUTPUTS, payload=payload, state_manager=state_manager, session="session"
    )

    assert result == {"subscription_id": "sub-1"}
    state_manager.push_state.assert_called_once_with(
        {"name": "bv-a", "crn": "crn:bv", "id": "bv-id", "cos_instance": "cos-a"}
    )
    ready.backup_vault_service.complete_backup_vault_create.assert_called_once_with("sub-1", TF_OUTPUTS, "session")
