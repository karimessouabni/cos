"""Tests des étapes du DAG ``cos.backup_vault.v1.update``."""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus


def vault_row(**overrides) -> dict:
    row = {
        "subscription_id": "bv-sub",
        "name": "bv-a",
        "region": "eu-fr2",
        "kms_crn": "crn:kms",
        "workspace": {"workspace_id": "ws-1"},
        "cos": {"crn": "crn:cos", "name": "cos-a", "context": {"realm": "realm-a", "app_code": "AP1"}},
    }
    row.update(overrides)
    return row


@pytest.fixture
def update_dag(load_dag):
    # Chemin relatif à dags/bucket/v1 : fonctionne aussi avec un conftest qui ne
    # cherche les DAGs que sous bucket/v1.
    return load_dag("../../backup_vault/v1/cos.backup_vault.v1.update.py")


@pytest.fixture
def payload(update_dag):
    return update_dag.module.CosBackupVaultUpdatePayload(subscription_id="bv-sub")


@pytest.fixture
def tf():
    tf = MagicMock(name="tf")
    tf.workspaces.get_by_id.return_value.apply.return_value.get_logs.return_value = {"tpl-1": "logs"}
    return tf


def test_dag_identity_and_steps(update_dag):
    assert update_dag.module.backup_vault_update.dag_name == "cos.backup_vault.v1.update"
    assert list(update_dag.steps) == ["validate_backup_vault", "update_tf_workspace", "update_backup_vault_in_db"]


class TestValidateBackupVault:
    def run(self, update_dag, payload):
        return update_dag.steps["validate_backup_vault"](session="session", payload=payload)

    def test_existing_vault_is_returned_as_a_dict(self, update_dag, services, payload):
        services.backup_vault_service.get_backup_vault_by_sub_id.return_value = vault_row()

        assert self.run(update_dag, payload) == vault_row()
        services.backup_vault_service.get_backup_vault_by_sub_id.assert_called_once_with("bv-sub", "session")

    @pytest.mark.parametrize("row", [None, vault_row(name=None), vault_row(name="")])
    def test_missing_or_unfinished_vault_is_declined(self, update_dag, services, payload, row):
        services.backup_vault_service.get_backup_vault_by_sub_id.return_value = row

        with pytest.raises(DeclineDemandException, match="doesn't exist or is not fully created for the sub id bv-sub"):
            self.run(update_dag, payload)


class TestUpdateTfWorkspace:
    def run(self, update_dag, payload, state_manager, tf):
        return update_dag.steps["update_tf_workspace"](
            backup_vault=vault_row(), state_manager=state_manager, tf=tf, reader="reader", payload=payload,
            vault="vault", session="session",
        )

    def test_refreshes_the_workspace_then_plans_and_applies(self, update_dag, services, payload, state_manager, tf):
        assert self.run(update_dag, payload, state_manager, tf) is True

        services.workspaceService.build_backup_vault_workspace_details.assert_called_once_with(
            workspace_id="ws-1", realm="realm-a", app_code="AP1", region="eu-fr2",
            cos_instance_crn="crn:cos", cos_instance_id="crn:cos", cos_instance_name="cos-a",
            kms_crn="crn:kms", description="my bucket",
        )
        services.backup_vault_service.update_backup_vault_status.assert_called_once_with(
            "bv-sub", SubscriptionStatus.ACTIVE, "session"
        )
        services.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
            "bv-sub", Status.INPROGRESS, "session"
        )
        services.workspaceService.update_backup_vault_workspace.assert_called_once_with(
            workspace_details=services.workspaceService.build_backup_vault_workspace_details.return_value,
            vault="vault", tf=tf, reader="reader",
        )
        tf.workspaces.get_by_id.assert_called_once_with("ws-1")
        workspace = tf.workspaces.get_by_id.return_value
        workspace.plan.assert_called_once_with()
        workspace.apply.assert_called_once_with()

    def test_failed_apply_locks_the_vault_and_reraises(self, update_dag, services, payload, state_manager, tf):
        tf.workspaces.get_by_id.return_value.apply.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(update_dag, payload, state_manager, tf)

        services.backup_vault_service.update_backup_vault_status.assert_called_with(
            "bv-sub", SubscriptionStatus.LOCKED, "session"
        )
        services.workspaceService.update_backup_vault_workspace_status.assert_called_with(
            "bv-sub", Status.FAILED, "session"
        )

    def test_failed_workspace_refresh_also_locks_the_vault(self, update_dag, services, payload, state_manager, tf):
        services.workspaceService.update_backup_vault_workspace.side_effect = RuntimeError("refresh failed")

        with pytest.raises(RuntimeError, match="refresh failed"):
            self.run(update_dag, payload, state_manager, tf)

        tf.workspaces.get_by_id.assert_not_called()
        services.backup_vault_service.update_backup_vault_status.assert_called_with(
            "bv-sub", SubscriptionStatus.LOCKED, "session"
        )


def test_update_backup_vault_in_db_saves_the_description_and_completes(update_dag, services, payload, state_manager):
    result = update_dag.steps["update_backup_vault_in_db"](
        apply_tf_result=True, payload=payload, state_manager=state_manager, session="session"
    )

    assert result == {"subscription_id": "bv-sub"}
    services.backup_vault_service.process_backup_vault_update.assert_called_once_with("bv-sub", "my bucket", "session")
    services.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
        "bv-sub", Status.SUCCESS, "session"
    )
