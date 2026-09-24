"""Tests des étapes du DAG ``cos.bucket.v1.restore``.

Le point de restauration vient du payload et doit tomber dans le recovery
range choisi. ``recovery_range_service`` est le vrai module.
"""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.models.BackupVaultRestore import BackupVaultRestore

RANGE_OLD = {
    "recovery_range_id": "old",
    "range_start_time": "2026-08-01T00:00:00.000Z",
    "range_end_time": "2026-09-10T00:00:00.000Z",
    "range_create_time": "2026-08-01T00:00:00.000Z",
}
RANGE_NEW = {
    "recovery_range_id": "new",
    "range_start_time": "2026-09-10T00:00:01.000Z",
    "range_end_time": "2026-09-16T08:00:00.000Z",
    "range_create_time": "2026-09-10T00:00:01.000Z",
}


def bucket_row(**overrides) -> dict:
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "bucket_crn": "crn:bucket-a",
        "clean_status": "success",
        "object_versioning_enabled": True,
        "region": "eu-de",
    }
    row.update(overrides)
    return row


def validated(**overrides) -> dict:
    data = {
        "source_bucket": bucket_row(),
        "target_bucket": bucket_row(name="bucket-b", bucket_crn="crn:bucket-b"),
        "backup_vault": {"subscription_id": "bv-sub", "name": "vault-a", "crn": "crn:bv"},
    }
    data.update(overrides)
    return data


@pytest.fixture
def restore_dag(load_dag):
    return load_dag("cos.bucket.v1.restore.py")


@pytest.fixture
def make_payload(restore_dag):
    def factory(**overrides):
        fields = dict(
            subscription_id="sub-1",
            requestor="karim",
            realm="realm-a",
            app_code="AP1",
            backup_vault_name="vault-a",
            target_bucket="bucket-b",
            restore_point_in_time="2026-09-05T10:30:00Z",
        )
        fields.update(overrides)
        return restore_dag.module.BucketRestoreBackupVaultPayload(**fields)

    return factory


@pytest.fixture
def happy_services(services):
    services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()
    services.bucketService.get_bucket_by_name.return_value = bucket_row(name="bucket-b", bucket_crn="crn:bucket-b")
    services.backup_vault_service.get_backup_vault_by_name.return_value = validated()["backup_vault"]
    services.restore_service.list_recovery_ranges.return_value = [RANGE_NEW, RANGE_OLD]
    return services


def test_dag_declares_the_expected_steps_in_order(restore_dag):
    assert list(restore_dag.steps) == [
        "input_user_validation",
        "validate_backup_vault",
        "get_wklapp_iam_token",
        "select_recovery_range",
        "create_tf_workspace_and_launch_restore",
    ]


class TestInputUserValidation:
    def run(self, restore_dag, payload):
        return restore_dag.steps["input_user_validation"](payload=payload, session="session")

    def errors_of(self, restore_dag, payload) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(restore_dag, payload)
        return str(excinfo.value).split(" | ")

    def test_valid_request(self, restore_dag, happy_services, make_payload):
        result = self.run(restore_dag, make_payload())

        assert result["source_bucket"]["name"] == "bucket-a"
        assert result["target_bucket"]["name"] == "bucket-b"
        assert result["backup_vault"]["name"] == "vault-a"

    def test_bad_restore_point_format_is_reported_with_the_other_errors(self, restore_dag, happy_services, make_payload):
        happy_services.backup_vault_service.get_backup_vault_by_name.return_value = None

        errors = self.errors_of(restore_dag, make_payload(restore_point_in_time="hier midi"))

        assert errors == [
            "The backup vault 'vault-a' does not exist.",
            "restore_point_in_time 'hier midi' is not a valid ISO 8601 date-time",
        ]

    def test_missing_restore_point_is_refused(self, restore_dag, happy_services, make_payload):
        errors = self.errors_of(restore_dag, make_payload(restore_point_in_time=None))

        assert errors == ["restore_point_in_time is required (ISO 8601, e.g. 2026-09-15T10:30:00Z)"]


class TestSelectRecoveryRange:
    def run(self, restore_dag, payload):
        return restore_dag.steps["select_recovery_range"](
            iam_token="tok", input_user_validation=validated(), payload=payload
        )

    def test_point_selects_the_range_that_covers_it(self, restore_dag, happy_services, make_payload):
        result = self.run(restore_dag, make_payload(restore_point_in_time="2026-09-05T12:30:00+02:00"))

        assert result["recovery_range"]["recovery_range_id"] == "old"
        assert result["restore_point_in_time"] == "2026-09-05T10:30:00.000Z"  # normalisé en UTC, format IBM
        happy_services.restore_service.list_recovery_ranges.assert_called_once_with("vault-a", "crn:bucket-a", "tok")

    def test_point_in_the_most_recent_range(self, restore_dag, happy_services, make_payload):
        result = self.run(restore_dag, make_payload(restore_point_in_time="2026-09-15T00:00:00Z"))

        assert result["recovery_range"]["recovery_range_id"] == "new"

    def test_point_outside_every_range_is_declined_with_the_windows(self, restore_dag, happy_services, make_payload):
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(restore_dag, make_payload(restore_point_in_time="2026-07-01T00:00:00Z"))

        message = str(excinfo.value)
        assert "not covered by any recovery range" in message
        assert "old [2026-08-01T00:00:00.000Z -> 2026-09-10T00:00:00.000Z]" in message
        assert "bucket bucket-a, vault vault-a" in message

    def test_explicit_range_not_covering_the_point_is_declined(self, restore_dag, happy_services, make_payload):
        with pytest.raises(DeclineDemandException, match="is outside recovery range new"):
            self.run(restore_dag, make_payload(restore_point_in_time="2026-09-05T10:30:00Z", recovery_range_id="new"))

    def test_explicit_range_covering_the_point(self, restore_dag, happy_services, make_payload):
        result = self.run(restore_dag, make_payload(restore_point_in_time="2026-09-05T10:30:00Z", recovery_range_id="old"))

        assert result["recovery_range"]["recovery_range_id"] == "old"

    def test_no_range_is_a_decline_not_a_failure(self, restore_dag, happy_services, make_payload):
        happy_services.restore_service.list_recovery_ranges.return_value = []

        with pytest.raises(DeclineDemandException, match="no recovery range exists"):
            self.run(restore_dag, make_payload())


class TestCreateTfWorkspaceAndLaunchRestore:
    @pytest.fixture
    def tf(self):
        backend = MagicMock(name="tf")
        backend.workspaces.get_by_name.return_value = []
        workspace = backend.workspaces.get_by_id.return_value
        workspace.get_outputs.return_value = [MagicMock(output_values=[{"restore_id": {"value": "rst-1"}}])]
        return backend

    def run(self, restore_dag, payload, tf, state_manager):
        return restore_dag.steps["create_tf_workspace_and_launch_restore"](
            iam_token="tok",
            input_user_validation=validated(),
            restore_target={"recovery_range": RANGE_OLD, "restore_point_in_time": "2026-09-05T10:30:00.000Z"},
            tf=tf, state_manager=state_manager, session="session", payload=payload, vault="vault",
        )

    def test_the_requested_point_is_sent_to_terraform_and_persisted(self, restore_dag, happy_services, make_payload, tf):
        happy_services.contextService.get_realm.return_value = {"name": "realm-a", "wklapp_account_number": "wk-1"}
        happy_services.vault_service.get_vault_secrets.return_value = {
            "vault_read_token": "rt", "vault_read_addr": "ra", "gitlab_token": "gl"
        }
        happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}
        happy_services.backup_vault_restore_repository.create_restore.return_value = BackupVaultRestore(id=7, status="requested")
        state_manager = MagicMock()
        state_manager.get_subscription.return_value.description = "restore"

        assert self.run(restore_dag, make_payload(), tf, state_manager) == "ws-1"

        variables = happy_services.schematics_service.create_or_update_ws.call_args.args[4]
        assert variables["restore_point_in_time"] == "2026-09-05T10:30:00.000Z"
        assert variables["restore_point_in_time"] != RANGE_OLD["range_end_time"]
        assert variables["recovery_range_id"] == "old"
        assert variables["target_resource_crn"] == "crn:bucket-b"
        row = happy_services.backup_vault_restore_repository.create_restore.call_args.args[1]
        assert isinstance(row, BackupVaultRestore)
        assert row.restore_point_in_time == "2026-09-05T10:30:00.000Z"
        assert row.recovery_range_id == "old"
        happy_services.backup_vault_restore_repository.mark_complete.assert_called_once()
        assert happy_services.backup_vault_restore_repository.mark_complete.call_args.args[2] == "rst-1"
