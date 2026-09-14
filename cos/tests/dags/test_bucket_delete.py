"""Tests des étapes du DAG ``cos.bucket.v1.delete``."""
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


def bucket_row(**overrides) -> dict:
    """Dict tel que le renvoie ``get_bucket_by_sub_id`` (relations incluses)."""
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "virtual_server_endpoint": "https://vpe/bucket-a",
        "region": "eu-de",
        "storage_class": "standard",
        "activity_tracker_crn": "crn:logs",
        "kms_crn": "crn:kms",
        "monitoring_crn": "crn:logs",
        "enable_custom_permissions": False,
        "description": "my bucket",
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "object_versioning_enabled": True,
        "retention_enabled": False,
        "retention_default": None,
        "retention_minimum": None,
        "retention_maximum": None,
        "backup_enabled": False,
        "backup_vault_subscription_id": None,
        "backup_retention_days": None,
        "backup_vault": None,
        "workspace": {"workspace_id": "ws-1"},
        "cos": {"crn": "crn:cos", "name": "cos-a", "context": {"realm": "realm-a", "app_code": "AP1"}},
    }
    row.update(overrides)
    return row


@pytest.fixture
def delete_dag(load_dag):
    return load_dag("cos.bucket.v1.delete.py")


@pytest.fixture
def payload(delete_dag):
    return delete_dag.module.BucketDeletePayload(subscription_id="sub-1")


@pytest.fixture
def tf():
    return MagicMock(name="tf")


def test_dag_declares_the_expected_steps_in_order(delete_dag):
    assert list(delete_dag.steps) == [
        "validate_request",
        "destroy_tf_resources",
        "update_db_for_resources",
        "destroy_tf_workspace",
        "update_db_for_workspace",
    ]


class TestValidateRequest:
    def run(self, delete_dag, payload):
        return delete_dag.steps["validate_request"](session="session", payload=payload, vault="vault")

    def errors_of(self, delete_dag, payload) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(delete_dag, payload)
        return str(excinfo.value).split(" | ")

    @pytest.fixture
    def empty_bucket(self, services):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()
        services.vault_service.get_cos_api_key.return_value = "api-key"
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"
        services.bucketService.check_bucket_has_contents.return_value = False
        return services

    def test_valid_request_returns_the_bucket_and_moves_it_to_terminating(self, delete_dag, empty_bucket, payload):
        result = self.run(delete_dag, payload)

        assert result == bucket_row()
        empty_bucket.bucketService.get_bucket_by_sub_id.assert_called_once_with("session", "sub-1")
        empty_bucket.vault_service.get_cos_api_key.assert_called_once_with(bucket_row(), "vault")
        empty_bucket.ibm_iam_service.get_iam_access_token.assert_called_once_with("api-key")
        empty_bucket.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())
        empty_bucket.bucketService.update_bucket_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.TERMINATING, "session"
        )

    def test_missing_bucket_is_declined_immediately(self, delete_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = None

        with pytest.raises(DeclineDemandException, match="doesn't exist for the sub id sub-1"):
            self.run(delete_dag, payload)

        services.vault_service.get_cos_api_key.assert_not_called()
        services.bucketService.update_bucket_status.assert_not_called()

    def test_not_fully_created_bucket_reports_every_missing_piece(self, delete_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(
            name=None, virtual_server_endpoint=None, workspace={"workspace_id": None}, cos=None
        )

        errors = self.errors_of(delete_dag, payload)

        assert errors == [
            "the bucket is not fully created for the sub id sub-1 (no name)",
            "the bucket has no endpoint, its contents cannot be checked",
            "the bucket workspace has no Schematics id, its resources cannot be destroyed",
            "the bucket is not linked to a cos instance",
        ]
        services.bucketService.check_bucket_has_contents.assert_not_called()
        services.bucketService.update_bucket_status.assert_not_called()

    def test_workspace_relation_absent_from_the_dict_is_reported_as_such(self, delete_dag, empty_bucket, payload):
        empty_bucket.bucketService.get_bucket_by_sub_id.return_value = bucket_row(workspace=None)

        errors = self.errors_of(delete_dag, payload)

        assert errors == ["the bucket row carries no workspace (relation not loaded or never created)"]
        empty_bucket.bucketService.update_bucket_status.assert_not_called()

    def test_non_empty_bucket_is_locked_and_declined(self, delete_dag, empty_bucket, payload):
        empty_bucket.bucketService.check_bucket_has_contents.return_value = True

        assert self.errors_of(delete_dag, payload) == ["The bucket bucket-a is not empty"]
        empty_bucket.bucketService.update_bucket_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.LOCKED, "session"
        )

    def test_non_empty_bucket_without_workspace_reports_both(self, delete_dag, empty_bucket, payload):
        empty_bucket.bucketService.get_bucket_by_sub_id.return_value = bucket_row(workspace={"workspace_id": None})
        empty_bucket.bucketService.check_bucket_has_contents.return_value = True

        errors = self.errors_of(delete_dag, payload)

        assert errors == [
            "the bucket workspace has no Schematics id, its resources cannot be destroyed",
            "The bucket bucket-a is not empty",
        ]

    def test_missing_workspace_alone_does_not_lock_the_bucket(self, delete_dag, empty_bucket, payload):
        empty_bucket.bucketService.get_bucket_by_sub_id.return_value = bucket_row(workspace={"workspace_id": None})

        self.errors_of(delete_dag, payload)

        empty_bucket.bucketService.update_bucket_status.assert_not_called()


class TestDestroyTfResources:
    def run(self, delete_dag, payload, tf, bucket=None):
        return delete_dag.steps["destroy_tf_resources"](
            bucket=bucket or bucket_row(), tf=tf, payload=payload, session="session", vault="vault"
        )

    def test_updates_the_workspace_then_destroys_its_resources(self, delete_dag, services, payload, tf):
        assert self.run(delete_dag, payload, tf) is True

        services.bucketService.update_bucket_on_destroy.assert_called_once_with("sub-1", "session")
        services.bucketService.update_bucket_workspace_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        details = services.workspaceService.build_bucket_workspace_details.call_args.kwargs
        assert details["workspace_id"] == "ws-1"
        assert details["realm"] == "realm-a"
        assert details["cos_instance_crn"] == "crn:cos"
        assert details["backup_vault_crn"] is None
        assert details["immutability"]["object_versioning_enabled"] is True
        assert details["immutability"]["immutability_choice"] == "none"
        assert "object_lock_duration_years" in details["immutability"]
        services.workspaceService.update_bucket_workspace.assert_called_once()
        tf.workspaces.get_by_id.assert_called_once_with(workspace_id="ws-1")
        call = tf.workspaces.delete_workspace_resources.call_args
        assert call.args == ("ws-1",)
        policy = call.kwargs["cooldown_policy"]
        assert (policy.delay, policy.max_attempts) == (5, 540)

    def test_backup_vault_crn_comes_from_the_bucket_dict(self, delete_dag, services, payload, tf):
        services.backup_vault_service.get_backup_vault_by_sub_id.return_value = MagicMock(subscription_id="bv-sub")
        bucket = bucket_row(
            backup_enabled=True, backup_vault_subscription_id="bv-sub", backup_retention_days=7,
            backup_vault={"subscription_id": "bv-sub", "crn": "crn:bv"},
        )

        self.run(delete_dag, payload, tf, bucket)

        details = services.workspaceService.build_bucket_workspace_details.call_args.kwargs
        assert details["backup_vault_crn"] == "crn:bv"
        assert details["immutability"]["backup"]["backup_vault_sub_id"] == "bv-sub"

    def test_missing_workspace_counts_as_destroyed(self, delete_dag, services, payload, tf):
        tf.workspaces.get_by_id.side_effect = SchematicsError(404)

        assert self.run(delete_dag, payload, tf) is True
        services.bucketService.update_bucket_status.assert_not_called()

    def test_other_error_locks_the_bucket_and_reraises(self, delete_dag, services, payload, tf):
        tf.workspaces.delete_workspace_resources.side_effect = SchematicsError(500)

        with pytest.raises(SchematicsError):
            self.run(delete_dag, payload, tf)

        services.bucketService.update_bucket_status.assert_called_once_with("sub-1", SubscriptionStatus.LOCKED, "session")
        services.bucketService.update_bucket_workspace_status.assert_called_with("sub-1", Status.FAILED, "session")


def test_update_db_for_resources_marks_the_destroy_action(delete_dag, services):
    assert delete_dag.steps["update_db_for_resources"](bucket=bucket_row(), destroyed_resources=True, session="session") is True
    services.bucketService.update_bucket_action.assert_called_once_with("sub-1", Action.DESTROY, "session")


class TestDestroyTfWorkspace:
    def run(self, delete_dag, tf, bucket=None):
        return delete_dag.steps["destroy_tf_workspace"](
            bucket=bucket or bucket_row(), update_db_resources_task=True, tf=tf, session="session"
        )

    def test_deletes_the_workspace(self, delete_dag, services, tf):
        assert self.run(delete_dag, tf) is True
        tf.workspaces.get_by_id.assert_called_once_with(workspace_id="ws-1")
        tf.workspaces.get_by_id.return_value.delete.assert_called_once()
        services.bucketService.update_bucket_workspace_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")

    def test_already_deleted_workspace_is_fine(self, delete_dag, services, tf):
        tf.workspaces.get_by_id.side_effect = SchematicsError(404)

        assert self.run(delete_dag, tf) is True
        services.bucketService.update_bucket_status.assert_not_called()

    def test_other_error_locks_the_bucket_and_reraises(self, delete_dag, services, tf):
        tf.workspaces.get_by_id.return_value.delete.side_effect = SchematicsError(500)

        with pytest.raises(SchematicsError):
            self.run(delete_dag, tf)

        services.bucketService.update_bucket_status.assert_called_once_with("sub-1", SubscriptionStatus.LOCKED, "session")


def test_update_db_for_workspace_terminates_the_bucket(delete_dag, services):
    assert delete_dag.steps["update_db_for_workspace"](bucket=bucket_row(), destroyed_workspace=True, session="session") is True
    services.bucketService.update_bucket_status.assert_called_once_with("sub-1", SubscriptionStatus.TERMINATED, "session")
    services.bucketService.update_bucket_workspace_status.assert_called_once_with("sub-1", Status.SUCCESS, "session")
