"""Tests des étapes du DAG ``cos.backup_vault.v1.delete``."""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.action import Action
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus


class SchematicsError(Exception):
    def __init__(self, code):
        super().__init__(f"schematics {code}")
        self.code = code


def vault_row(**overrides) -> dict:
    """Dict tel que le renvoie ``get_backup_vault_by_sub_id`` (relations incluses)."""
    row = {
        "subscription_id": "bv-sub",
        "name": "bv-a",
        "region": "eu-fr2",
        "kms_crn": "crn:kms",
        "description": "my vault",
        "workspace": {"workspace_id": "ws-1"},
        "cos": {"crn": "crn:cos", "name": "cos-a", "context": {"realm": "realm-a", "app_code": "AP1"}},
    }
    row.update(overrides)
    return row


@pytest.fixture
def delete_dag(load_dag):
    # Chemin relatif à dags/bucket/v1 : fonctionne aussi avec un conftest qui ne
    # cherche les DAGs que sous bucket/v1.
    return load_dag("../../backup_vault/v1/cos.backup_vault.v1.delete.py")


@pytest.fixture
def payload(delete_dag):
    return delete_dag.module.CosBackupVaultDeletePayload(subscription_id="bv-sub")


@pytest.fixture
def tf():
    return MagicMock(name="tf")


@pytest.fixture
def clean_vault(services):
    """Un vault complet, sans recovery range."""
    services.backup_vault_service.get_backup_vault_by_sub_id.return_value = vault_row()
    services.vault_service.get_vault_secrets.return_value = {"wklapp_account_api_key": "api-key"}
    services.ibm_iam_service.get_iam_access_token.return_value = "tok"
    services.backup_vault_service.check_backup_vault_has_active_recovery_ranges.return_value = ([], [])
    return services


def test_dag_identity_and_steps(delete_dag):
    assert delete_dag.module.backup_vault_delete.dag_name == "cos.backup_vault.v1.delete"
    assert delete_dag.module.backup_vault_delete.config.options == {"lock_subscription_on_failure": False}
    assert list(delete_dag.steps) == [
        "validate_backup_vault_and_workspace",
        "destroy_tf_resources",
        "update_db_for_resources",
        "destroy_tf_workspace",
        "update_db_for_workspace",
    ]


class TestValidateBackupVaultAndWorkspace:
    def run(self, delete_dag, payload):
        return delete_dag.steps["validate_backup_vault_and_workspace"](
            session="session", payload=payload, reader="reader", vault="vault"
        )

    def errors_of(self, delete_dag, payload) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(delete_dag, payload)
        return str(excinfo.value).split(" | ")

    def test_valid_vault_is_returned(self, delete_dag, clean_vault, payload):
        assert self.run(delete_dag, payload) == vault_row()

        clean_vault.backup_vault_service.get_backup_vault_by_sub_id.assert_called_once_with("bv-sub", "session")
        clean_vault.vault_service.get_vault_secrets.assert_called_once_with(
            realm_name="realm-a", apcode="AP1", vault="vault", reader="reader"
        )
        clean_vault.ibm_iam_service.get_iam_access_token.assert_called_once_with("api-key")
        clean_vault.backup_vault_service.check_backup_vault_has_active_recovery_ranges.assert_called_once_with(
            "tok", vault_row()
        )

    def test_missing_vault_is_declined_without_crashing(self, delete_dag, clean_vault, payload):
        """L'original faisait dict(None) puis indexait ["cos"] sur None."""
        clean_vault.backup_vault_service.get_backup_vault_by_sub_id.return_value = None

        with pytest.raises(DeclineDemandException, match="doesn't exist for the sub id bv-sub"):
            self.run(delete_dag, payload)

        clean_vault.vault_service.get_vault_secrets.assert_not_called()

    def test_vault_without_name_is_declined_before_checking_ranges(self, delete_dag, clean_vault, payload):
        clean_vault.backup_vault_service.get_backup_vault_by_sub_id.return_value = vault_row(name=None)

        assert self.errors_of(delete_dag, payload) == [
            "the backup vault is not fully created for the sub id bv-sub (no name)"
        ]
        clean_vault.backup_vault_service.check_backup_vault_has_active_recovery_ranges.assert_not_called()

    def test_vault_without_cos_is_declined(self, delete_dag, clean_vault, payload):
        clean_vault.backup_vault_service.get_backup_vault_by_sub_id.return_value = vault_row(cos=None)

        assert self.errors_of(delete_dag, payload) == ["the backup vault is not linked to a cos instance"]

    def test_active_recovery_ranges_block_the_deletion(self, delete_dag, clean_vault, payload):
        clean_vault.backup_vault_service.check_backup_vault_has_active_recovery_ranges.return_value = (["r1"], [])

        assert self.errors_of(delete_dag, payload) == [
            "The backup vault bv-a still has active recovery ranges. You cannot delete it."
        ]

    def test_expired_recovery_ranges_block_the_deletion_too(self, delete_dag, clean_vault, payload):
        clean_vault.backup_vault_service.check_backup_vault_has_active_recovery_ranges.return_value = (["r1"], ["r0"])

        errors = self.errors_of(delete_dag, payload)

        assert len(errors) == 2
        assert errors[1].startswith("The backup vault bv-a still has some expired recovery ranges.")


class TestDestroyTfResources:
    def run(self, delete_dag, payload, tf, backup_vault=None):
        return delete_dag.steps["destroy_tf_resources"](
            backup_vault=backup_vault or vault_row(), tf=tf, payload=payload, reader="reader", session="session",
            vault="vault",
        )

    def test_refreshes_the_workspace_then_destroys_its_resources(self, delete_dag, services, payload, tf):
        assert self.run(delete_dag, payload, tf) is True

        services.workspaceService.build_backup_vault_workspace_details.assert_called_once_with(
            workspace_id="ws-1", realm="realm-a", app_code="AP1", region="eu-fr2",
            cos_instance_crn="crn:cos", cos_instance_id="crn:cos", cos_instance_name="cos-a",
            kms_crn="crn:kms", description="my vault",
        )
        services.backup_vault_service.update_backup_vault_on_destroy.assert_called_once_with("bv-sub", "session")
        services.backup_vault_service.update_backup_vault_status.assert_called_once_with(
            "bv-sub", SubscriptionStatus.TERMINATING, "session"
        )
        services.workspaceService.update_backup_vault_workspace.assert_called_once_with(
            workspace_details=services.workspaceService.build_backup_vault_workspace_details.return_value,
            vault="vault", tf=tf, reader="reader",
        )
        tf.workspaces.get_by_id.assert_called_once_with(workspace_id="ws-1")
        call = tf.workspaces.delete_workspace_resources.call_args
        assert call.args == ("ws-1",)
        policy = call.kwargs["cooldown_policy"]
        assert (policy.delay, policy.max_attempts) == (5, 540)

    def test_missing_workspace_counts_as_destroyed(self, delete_dag, services, payload, tf):
        tf.workspaces.get_by_id.side_effect = SchematicsError(404)

        assert self.run(delete_dag, payload, tf) is True
        tf.workspaces.delete_workspace_resources.assert_not_called()
        statuses = [c.args[1] for c in services.backup_vault_service.update_backup_vault_status.call_args_list]
        assert statuses == [SubscriptionStatus.TERMINATING]

    def test_other_error_locks_the_vault_and_reraises(self, delete_dag, services, payload, tf):
        tf.workspaces.delete_workspace_resources.side_effect = SchematicsError(500)

        with pytest.raises(SchematicsError):
            self.run(delete_dag, payload, tf)

        services.backup_vault_service.update_backup_vault_status.assert_called_with(
            "bv-sub", SubscriptionStatus.LOCKED, "session"
        )
        services.workspaceService.update_backup_vault_workspace_status.assert_called_with(
            "bv-sub", Status.FAILED, "session"
        )

    def test_failed_workspace_refresh_also_locks_the_vault(self, delete_dag, services, payload, tf):
        services.workspaceService.update_backup_vault_workspace.side_effect = RuntimeError("schematics down")

        with pytest.raises(RuntimeError, match="schematics down"):
            self.run(delete_dag, payload, tf)

        tf.workspaces.delete_workspace_resources.assert_not_called()
        services.backup_vault_service.update_backup_vault_status.assert_called_with(
            "bv-sub", SubscriptionStatus.LOCKED, "session"
        )


def test_update_db_for_resources_marks_the_destroy_action(delete_dag, services):
    from cos_service.models.BackupVault import BackupVault

    session = MagicMock(name="session")

    assert delete_dag.steps["update_db_for_resources"](
        backup_vault=vault_row(), destroyed_resources=True, session=session
    ) is True

    statement = session.execute.call_args.args[0]
    assert statement.table is BackupVault
    assert statement.values_ == {"action": Action.DESTROY.value}
    assert statement.where_ == (BackupVault.subscription_id == "bv-sub")
    session.commit.assert_called_once()


class TestDestroyTfWorkspace:
    def run(self, delete_dag, tf, backup_vault=None):
        return delete_dag.steps["destroy_tf_workspace"](
            backup_vault=backup_vault or vault_row(), update_db_resources_task=True, tf=tf, session="session"
        )

    def test_deletes_the_workspace(self, delete_dag, services, tf):
        assert self.run(delete_dag, tf) is True

        services.backup_vault_service.update_backup_vault_status.assert_called_once_with(
            "bv-sub", SubscriptionStatus.TERMINATING, "session"
        )
        services.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
            "bv-sub", Status.INPROGRESS, "session"
        )
        tf.workspaces.get_by_id.assert_called_once_with(workspace_id="ws-1")
        tf.workspaces.get_by_id.return_value.delete.assert_called_once()

    def test_vault_without_workspace_has_nothing_to_delete(self, delete_dag, services, tf):
        assert self.run(delete_dag, tf, vault_row(workspace={"workspace_id": None})) is True

        tf.workspaces.get_by_id.assert_not_called()
        services.backup_vault_service.update_backup_vault_status.assert_not_called()

    def test_already_deleted_workspace_is_fine(self, delete_dag, services, tf):
        tf.workspaces.get_by_id.side_effect = SchematicsError(404)

        assert self.run(delete_dag, tf) is True
        services.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
            "bv-sub", Status.INPROGRESS, "session"
        )

    def test_other_error_locks_the_vault_and_reraises(self, delete_dag, services, tf):
        tf.workspaces.get_by_id.return_value.delete.side_effect = SchematicsError(500)

        with pytest.raises(SchematicsError):
            self.run(delete_dag, tf)

        services.backup_vault_service.update_backup_vault_status.assert_called_with(
            "bv-sub", SubscriptionStatus.LOCKED, "session"
        )
        services.workspaceService.update_backup_vault_workspace_status.assert_called_with(
            "bv-sub", Status.FAILED, "session"
        )


def test_update_db_for_workspace_terminates_the_vault(delete_dag, services):
    assert delete_dag.steps["update_db_for_workspace"](
        backup_vault=vault_row(), destroyed_workspace=True, session="session"
    ) is True

    services.backup_vault_service.update_backup_vault_status.assert_called_once_with(
        "bv-sub", SubscriptionStatus.TERMINATED, "session"
    )
    services.workspaceService.update_backup_vault_workspace_status.assert_called_once_with(
        "bv-sub", Status.SUCCESS, "session"
    )
