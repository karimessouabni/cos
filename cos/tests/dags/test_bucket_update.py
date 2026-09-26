"""Tests des étapes du DAG ``cos.bucket.v1.update``.

``immutability_service`` est le vrai module : les règles d'update sont
exercées à travers ``validate_request``.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.bucket_retention import BucketRetention
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus


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
        "enable_custom_permissions": True,
        "description": "my bucket",
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "object_versioning_enabled": False,
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
def update_dag(load_dag):
    return load_dag("cos.bucket.v1.update.py")


@pytest.fixture
def make_payload(update_dag):
    def factory(**overrides):
        fields = dict(subscription_id="sub-1")
        fields.update(overrides)
        return update_dag.module.BucketUpdatePayload(**fields)

    return factory


@pytest.fixture
def state_manager():
    manager = MagicMock(name="state_manager")
    manager.get_subscription.return_value.description = "my bucket"
    return manager


@pytest.fixture
def empty_bucket(services):
    services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()
    services.vault_service.get_cos_api_key.return_value = "api-key"
    services.ibm_iam_service.get_iam_access_token.return_value = "tok"
    services.bucketService.check_bucket_has_contents.return_value = False
    services.backup_vault_service.get_backup_vault_by_name.return_value = {"subscription_id": "bv-sub", "crn": "crn:bv"}
    services.backup_vault_service.get_backup_vault_by_sub_id.return_value = SimpleNamespace(subscription_id="bv-sub", crn="crn:bv")
    return services


def test_dag_declares_the_expected_steps_in_order(update_dag):
    assert list(update_dag.steps) == [
        "validate_request",
        "compute_target_time",
        "update_tf_workspace",
        "save_bucket_in_db",
    ]


def test_payload_defaults(make_payload):
    payload = make_payload()

    assert payload.retention is None
    assert payload.backup is None
    assert payload.enable_custom_permissions is None
    assert payload.scheduling_update_date_time == "2025-06-03T12:13:02.000"


def test_compute_target_time_is_iso8601_in_paris_time(update_dag, make_payload):
    pytest.importorskip("pendulum", reason="pendulum (dépendance Airflow) absent de cet environnement")
    result = update_dag.steps["compute_target_time"](payload=make_payload(scheduling_update_date_time="2026-01-15T10:00:00.000"))

    assert result == "2026-01-15T10:00:00+01:00"


class TestValidateRequest:
    def run(self, update_dag, payload):
        return update_dag.steps["validate_request"](session="session", payload=payload, vault="vault")

    def errors_of(self, update_dag, payload) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(update_dag, payload)
        return str(excinfo.value).split(" | ")

    def test_versioning_toggle_on_a_plain_bucket(self, update_dag, empty_bucket, make_payload):
        result = self.run(update_dag, make_payload(enable_versioning=True))

        assert result["bucket"] == bucket_row()
        assert result["immutability"]["object_versioning_enabled"] is True
        assert result["immutability"]["immutability_choice"] == "none"
        assert result["backup_vault_crn"] is None
        assert result["enable_custom_permissions"] is True  # repli sur la valeur en base
        empty_bucket.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())

    def test_custom_permissions_from_the_payload_win(self, update_dag, empty_bucket, make_payload):
        result = self.run(update_dag, make_payload(enable_custom_permissions=False))

        assert result["enable_custom_permissions"] is False

    def test_missing_bucket_is_declined_immediately(self, update_dag, services, make_payload):
        services.bucketService.get_bucket_by_sub_id.return_value = None

        with pytest.raises(DeclineDemandException, match="doesn't exist for the sub id sub-1"):
            self.run(update_dag, make_payload())

        services.vault_service.get_cos_api_key.assert_not_called()

    def test_not_fully_created_bucket_reports_every_missing_piece(self, update_dag, services, make_payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(
            name=None, virtual_server_endpoint=None, workspace={"workspace_id": None}, cos=None
        )

        errors = self.errors_of(update_dag, make_payload())

        assert errors == [
            "the bucket is not fully created for the sub id sub-1 (no name)",
            "the bucket has no endpoint, its contents cannot be checked",
            "the bucket has no Terraform workspace, it cannot be updated",
            "the bucket is not linked to a cos instance",
        ]
        services.bucketService.check_bucket_has_contents.assert_not_called()

    def test_backup_without_vault_name(self, update_dag, empty_bucket, make_payload):
        errors = self.errors_of(update_dag, make_payload(backup=BucketBackup(backup_enabled=True, backup_retention_days=7)))

        assert "The Backup Vault name is required to enable bucket backup" in errors

    def test_unknown_backup_vault(self, update_dag, empty_bucket, make_payload):
        empty_bucket.backup_vault_service.get_backup_vault_by_name.return_value = None
        payload = make_payload(backup=BucketBackup(backup_enabled=True, backup_vault_name="nope", backup_retention_days=7))

        errors = self.errors_of(update_dag, payload)

        assert "No Backup Vault exist with the name : nope" in errors

    def test_backup_enabled_resolves_the_vault_crn(self, update_dag, empty_bucket, make_payload):
        payload = make_payload(
            enable_versioning=True,
            backup=BucketBackup(backup_enabled=True, backup_vault_name="vault-a", backup_retention_days=7),
        )

        result = self.run(update_dag, payload)

        assert result["immutability"]["backup"] == {
            "backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7
        }
        assert result["backup_vault_crn"] == "crn:bv"
        empty_bucket.backup_vault_service.get_backup_vault_by_sub_id.assert_called_once_with("bv-sub", "session")

    def test_service_errors_are_merged_with_dag_errors(self, update_dag, empty_bucket, make_payload):
        """Bucket object-locké sans workspace, et le client demande une rétention."""
        empty_bucket.bucketService.get_bucket_by_sub_id.return_value = bucket_row(
            object_lock_duration_days=30, object_versioning_enabled=True, workspace={"workspace_id": None}
        )
        payload = make_payload(retention=BucketRetention(retention_enabled=True, default_days=30, minimum_days=10, maximum_days=60))

        errors = self.errors_of(update_dag, payload)

        assert errors == [
            "the bucket has no Terraform workspace, it cannot be updated",
            "Setting a retention is not possible when object-lock is already enabled.",
        ]

    def test_retention_bucket_with_contents_is_declined(self, update_dag, empty_bucket, make_payload):
        empty_bucket.bucketService.get_bucket_by_sub_id.return_value = bucket_row(
            retention_enabled=True, retention_default=30, retention_minimum=10, retention_maximum=60
        )
        empty_bucket.bucketService.check_bucket_has_contents.return_value = True

        errors = self.errors_of(update_dag, make_payload())

        assert errors == ["Setting a retention is not possible when the bucket already contains objects."]


def validated(**overrides) -> dict:
    data = {
        "bucket": bucket_row(),
        "immutability": {
            "immutability_choice": "object_lock",
            "object_locking_enabled": True,
            "object_versioning_enabled": True,
            "object_lock_duration_days": 30,
            "object_lock_duration_years": None,
            "retention": {"retention_enabled": False, "default": None, "minimum": None, "maximum": None},
            "backup": {"backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7},
        },
        "backup_vault_crn": "crn:bv",
        "enable_custom_permissions": False,
    }
    data.update(overrides)
    return data


class TestUpdateTfWorkspace:
    def run(self, update_dag, payload, state_manager):
        return update_dag.steps["update_tf_workspace"](
            validated=validated(), state_manager=state_manager, payload=payload, tf="tf", vault="vault", session="session"
        )

    def test_refreshes_the_workspace_then_runs_it(self, update_dag, services, make_payload, state_manager):
        assert self.run(update_dag, make_payload(), state_manager) is True

        details = services.workspaceService.build_bucket_workspace_details.call_args.kwargs
        assert details["workspace_id"] == "ws-1"
        assert details["realm"] == "realm-a"
        assert details["immutability"]["object_lock_duration_days"] == 30
        assert details["enable_custom_permissions"] is False
        assert details["backup_vault_crn"] == "crn:bv"
        assert details["description"] == "my bucket"
        services.bucketService.update_bucket_status.assert_called_once_with("sub-1", SubscriptionStatus.ACTIVE, "session")
        services.bucketService.update_bucket_workspace_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        services.workspaceService.update_bucket_workspace.assert_called_once()
        services.schematics_service.run_workspace.assert_called_once_with("tf", "ws-1")
        services.backup_vault_service.get_backup_vault_by_sub_id.assert_not_called()

    def test_failed_apply_locks_the_bucket_and_reraises(self, update_dag, services, make_payload, state_manager):
        services.schematics_service.run_workspace.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(update_dag, make_payload(), state_manager)

        services.bucketService.update_bucket_status.assert_called_with("sub-1", SubscriptionStatus.LOCKED, "session")
        services.bucketService.update_bucket_workspace_status.assert_called_with("sub-1", Status.FAILED, "session")


class TestSaveBucketInDb:
    def test_pushes_the_effective_configuration_once(self, update_dag, services, make_payload, state_manager):
        data = validated()

        result = update_dag.steps["save_bucket_in_db"](
            validated=data, is_update_ws_done=True, payload=make_payload(), state_manager=state_manager, session="session"
        )

        assert result == {"subscription_id": "sub-1"}
        state_manager.push_state.assert_called_once_with({
            "retention": data["immutability"]["retention"],
            "object_lock_duration_days": 30,
            "object_lock_duration_years": None,
            "object_locking_enabled": True,
            "enable_versioning": True,
            "backup": data["immutability"]["backup"],
            "enable_custom_permissions": False,
            "immutability_choice": "object_lock",
        })
        services.bucketService.process_bucket_update.assert_called_once_with(
            "sub-1", data["immutability"], False, "my bucket", "session"
        )
        services.bucketService.update_bucket_workspace_status.assert_called_once_with("sub-1", Status.SUCCESS, "session")

    def test_retention_state_echoes_the_unit_sent_by_the_client(self, update_dag, services, make_payload, state_manager):
        data = validated()
        data["immutability"]["retention"] = {"retention_enabled": True, "default": 365, "minimum": 365, "maximum": 730}
        payload = make_payload(retention=BucketRetention(
            retention_enabled=True, default_years=1, minimum_years=1, maximum_years=2
        ))

        update_dag.steps["save_bucket_in_db"](
            validated=data, is_update_ws_done=True, payload=payload, state_manager=state_manager, session="session"
        )

        assert state_manager.push_state.call_args.args[0]["retention"] == {
            "retention_enabled": True, "default": 365, "minimum": 365, "maximum": 730,
            "unit": "years", "default_years": 1, "minimum_years": 1, "maximum_years": 2,
        }
