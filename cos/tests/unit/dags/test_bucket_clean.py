"""Tests des étapes du DAG ``cos.bucket.v1.clean`` (période de grâce v1, sans quarantaine)."""
from datetime import datetime, timedelta, timezone

import pytest

from airflow.sensors.base import PokeReturnValue

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.clean_status import CleanStatus


def bucket_row(**overrides) -> dict:
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "clean_status": CleanStatus.SUCCESS.value,
        "clean_execute_at": None,
        "virtual_server_endpoint": "https://vpe/bucket-a",
        "retention_enabled": False,
        "retention_maximum": None,
        "retention_default": None,
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "workspace": {"workspace_id": "ws-1"},
        "cos": {"crn": "crn:cos"},
    }
    row.update(overrides)
    return row


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


class S3Error(Exception):
    def __init__(self, code):
        super().__init__(f"s3 {code}")
        self.code = code


@pytest.fixture
def clean_dag(load_dag):
    return load_dag("cos.bucket.v1.clean.py")


@pytest.fixture
def payload(clean_dag):
    return clean_dag.module.BucketCleanPayload(subscription_id="sub-1", requestor="karim")


@pytest.fixture
def s3(services):
    services.ibm_iam_service.get_iam_access_token.return_value = "tok"
    return services


def assert_failed(services, state_manager):
    services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", CleanStatus.FAILED, "session")
    state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.FAILED.value})


def test_dag_identity(clean_dag):
    assert clean_dag.module.bucket_clean.dag_name == "cos.bucket.v1.clean"
    assert clean_dag.module.bucket_clean.config.options == {"lock_subscription_on_failure": False}
    assert list(clean_dag.steps) == [
        "validate_bucket",
        "check_locks",
        "schedule_clean",
        "claim_clean",
        "get_cos_api_key",
        "is_bucket_empty",
        "create_expiration_rule",
        "save_create_expiration_rule_in_db",
        "scheduler_clean_bucket",
        "delete_expiration_rule",
        "save_delete_expiration_rule_in_db",
        "complete_clean",
    ]


def test_wiring_waits_for_the_grace_period_between_schedule_and_claim(clean_dag):
    """Le sensor de date est câblé entre la programmation et la décision ; il lit la
    date renvoyée par schedule_clean. Airflow ne déduit l'ordre que des valeurs
    consommées : chaque étape de base est consommée par la suivante."""
    import inspect

    source = inspect.getsource(clean_dag.module)
    assert 'task_id="wait_for_grace_period"' in source
    assert "xcom_pull(task_ids='%s') }}\" % execute_at.operator.task_id" in source
    assert "execute_at >> wait" in source and "wait >> claimed" in source
    assert "schedule_clean(bucket=bucket, locked_until=lock_check)" in source
    assert "claim_clean(bucket=bucket, execute_at=execute_at)" in source
    assert "get_cos_api_key(bucket=bucket, claimed=claimed)" in source
    # Un step.sensor refuse un mot-clé supplémentaire : il consomme le drapeau renvoyé par la sauvegarde.
    assert "scheduler_clean_bucket(bucket=bucket, api_key=api_key, is_expiration_created=rule_saved)" in source
    assert "complete_clean(bucket=bucket, rules_saved=rules_saved)" in source


def test_only_the_scheduler_is_a_sensor_with_a_bounded_wait(clean_dag):
    assert clean_dag.steps["scheduler_clean_bucket"].sensor_options == {
        "exponential_backoff": False, "poke_interval": 3 * 3600, "timeout": 7 * 24 * 3600, "mode": "reschedule",
    }
    assert all(clean_dag.steps[name].sensor_options is None for name in clean_dag.steps if name != "scheduler_clean_bucket")


class TestGracePeriod:
    def test_defaults_to_seven_days(self, clean_dag, monkeypatch):
        monkeypatch.delenv("COS_CLEAN_GRACE_MINUTES", raising=False)

        assert clean_dag.module.grace_period() == timedelta(days=7)

    def test_is_set_in_minutes_for_the_toolchain_tests(self, clean_dag, monkeypatch):
        monkeypatch.setenv("COS_CLEAN_GRACE_MINUTES", "5")

        assert clean_dag.module.grace_period() == timedelta(minutes=5)


class TestLockPolicy:
    """La borne de fin des verrous : LastModified le plus récent + durée maximale."""

    def test_no_policy_means_nothing_locked(self, clean_dag):
        assert clean_dag.module.lock_policy(bucket_row()) is None
        assert clean_dag.module.locked_until(bucket_row(), utc(2026, 10, 1)) is None

    def test_retention_uses_the_maximum_in_days(self, clean_dag):
        row = bucket_row(retention_enabled=True, retention_maximum=30, retention_default=10)

        assert clean_dag.module.lock_policy(row)[0] == "retention"
        assert clean_dag.module.locked_until(row, utc(2026, 10, 1)) == utc(2026, 10, 31)

    def test_retention_falls_back_to_the_default_then_to_unknown(self, clean_dag):
        assert clean_dag.module.locked_until(
            bucket_row(retention_enabled=True, retention_default=10), utc(2026, 10, 1)
        ) == utc(2026, 10, 11)
        assert clean_dag.module.lock_policy(bucket_row(retention_enabled=True)) == ("retention", None)

    def test_object_lock_in_days_or_years(self, clean_dag):
        assert clean_dag.module.locked_until(
            bucket_row(object_lock_duration_days=45), utc(2026, 10, 1)
        ) == utc(2026, 11, 15)
        assert clean_dag.module.locked_until(
            bucket_row(object_lock_duration_years=1), utc(2024, 2, 29)
        ) == utc(2025, 2, 28)  # année bissextile : relativedelta, pas 365 jours

    def test_empty_bucket_has_no_lock_to_wait_for(self, clean_dag):
        assert clean_dag.module.locked_until(bucket_row(retention_enabled=True, retention_maximum=30), None) is None


class TestValidateBucket:
    def run(self, clean_dag, payload):
        return clean_dag.steps["validate_bucket"](session="session", payload=payload)

    def test_returns_the_bucket_without_touching_anything(self, clean_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert self.run(clean_dag, payload) == bucket_row()

        services.bucketService.get_bucket_by_sub_id.assert_called_once_with("session", "sub-1")
        services.bucketService.update_bucket_clean_status.assert_not_called()
        services.bucketService.schedule_bucket_clean.assert_not_called()

    @pytest.mark.parametrize("row", [None, bucket_row(name=None), bucket_row(name="")])
    def test_missing_or_unfinished_bucket_is_declined(self, clean_dag, services, payload, row):
        services.bucketService.get_bucket_by_sub_id.return_value = row

        with pytest.raises(DeclineDemandException, match="doesn't exist or not fully created for the sub id sub-1"):
            self.run(clean_dag, payload)

    @pytest.mark.parametrize("status", [CleanStatus.SCHEDULED.value, CleanStatus.INPROGRESS.value])
    def test_a_clean_already_scheduled_or_running_is_declined(self, clean_dag, services, payload, status):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(
            clean_status=status, clean_execute_at="2026-10-13T10:00:00+00:00"
        )

        with pytest.raises(DeclineDemandException, match=f"already {status}.*2026-10-13.*cancel it first"):
            self.run(clean_dag, payload)

    @pytest.mark.parametrize("status", [CleanStatus.SUCCESS.value, CleanStatus.FAILED.value, CleanStatus.CANCELLED.value, None])
    def test_a_finished_clean_does_not_block_a_new_one(self, clean_dag, services, payload, status):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_status=status)

        assert self.run(clean_dag, payload)["clean_status"] == status

    @pytest.mark.parametrize("locked", [
        {"retention_enabled": True, "retention_maximum": 30},
        {"object_lock_duration_days": 30},
    ])
    def test_locks_are_not_judged_from_the_database_alone(self, clean_dag, services, payload, locked):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(**locked)

        assert self.run(clean_dag, payload) == bucket_row(**locked)

    def test_no_endpoint_means_no_listing_so_declined(self, clean_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(virtual_server_endpoint=None)

        with pytest.raises(DeclineDemandException, match="no endpoint"):
            self.run(clean_dag, payload)


class TestCheckLocks:
    def run(self, clean_dag, row):
        return clean_dag.steps["check_locks"](bucket=row, vault="vault", reader="reader")

    @pytest.fixture
    def listing(self, services):
        services.vault_service.get_cos_api_key.return_value = "api-key"
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"
        services.schematics_service.setting.return_value = str(7 * 24 * 60)
        return services

    def test_without_a_policy_nothing_is_listed(self, clean_dag, listing):
        assert self.run(clean_dag, bucket_row()) is None

        listing.bucketService.latest_object_modification.assert_not_called()
        listing.vault_service.get_cos_api_key.assert_not_called()

    def test_locks_ending_before_the_execution_are_accepted(self, clean_dag, listing):
        row = bucket_row(retention_enabled=True, retention_maximum=30)
        listing.bucketService.latest_object_modification.return_value = datetime.now(timezone.utc) - timedelta(days=29)

        until = self.run(clean_dag, row)

        assert datetime.fromisoformat(until) < datetime.now(timezone.utc) + timedelta(days=7)
        listing.vault_service.get_cos_api_key.assert_called_once_with(row, "vault", "reader")
        listing.bucketService.latest_object_modification.assert_called_once_with("tok", row, versions=False)

    def test_object_lock_lists_the_versions(self, clean_dag, listing):
        row = bucket_row(object_lock_duration_days=3)
        listing.bucketService.latest_object_modification.return_value = datetime.now(timezone.utc) - timedelta(days=2)

        assert self.run(clean_dag, row) is not None

        listing.bucketService.latest_object_modification.assert_called_once_with("tok", row, versions=True)

    def test_locks_ending_after_the_grace_period_are_declined_with_the_date(self, clean_dag, listing):
        row = bucket_row(retention_enabled=True, retention_maximum=30)
        latest = datetime.now(timezone.utc) - timedelta(days=1)
        listing.bucketService.latest_object_modification.return_value = latest

        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(clean_dag, row)

        message = str(excinfo.value)
        assert (latest + timedelta(days=30)).isoformat() in message
        assert "after the end of the grace period" in message
        assert "request the clean again after that date" in message

    def test_a_shorter_grace_period_makes_the_same_locks_a_refusal(self, clean_dag, listing):
        listing.schematics_service.setting.return_value = "5"  # minutes
        row = bucket_row(object_lock_duration_days=3)
        listing.bucketService.latest_object_modification.return_value = datetime.now(timezone.utc) - timedelta(days=2)

        with pytest.raises(DeclineDemandException, match="may stay locked until"):
            self.run(clean_dag, row)

    def test_empty_locked_bucket_is_accepted(self, clean_dag, listing):
        listing.bucketService.latest_object_modification.return_value = None

        assert self.run(clean_dag, bucket_row(retention_enabled=True, retention_maximum=30)) is None

    def test_retention_without_known_duration_is_declined(self, clean_dag, listing):
        with pytest.raises(DeclineDemandException, match="without a known maximum duration"):
            self.run(clean_dag, bucket_row(retention_enabled=True))

        listing.bucketService.latest_object_modification.assert_not_called()


class TestScheduleClean:
    def test_marks_scheduled_with_the_execution_date_and_returns_it(self, clean_dag, services, state_manager):
        # La grâce est lue par schematics_service.setting (Airflow Variable, env, défaut) : ici doublé.
        services.schematics_service.setting.return_value = str(7 * 24 * 60)
        before = datetime.now(timezone.utc)

        execute_at = clean_dag.steps["schedule_clean"](
            bucket=bucket_row(), locked_until=None, state_manager=state_manager, session="session"
        )

        args = services.bucketService.schedule_bucket_clean.call_args.args
        assert args[0] == "sub-1" and args[3] == "session"
        requested_at, planned = args[1], args[2]
        assert before <= requested_at <= datetime.now(timezone.utc)
        assert planned - requested_at == timedelta(days=7)
        assert planned.tzinfo is not None
        assert execute_at == planned.isoformat()
        state = state_manager.push_state.call_args.args[0]
        assert state["clean_status"] == CleanStatus.SCHEDULED.value
        assert state["clean_requested_at"] == requested_at.isoformat()
        assert state["clean_execute_at"] == planned.isoformat()
        assert planned.isoformat() in state["clean_notice"] and "Cancel the clean" in state["clean_notice"]
        services.schematics_service.setting.assert_called_once_with("cos_clean_grace_minutes", "10080")


class TestClaimClean:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["claim_clean"](
            bucket=bucket_row(), execute_at="2026-10-13T10:00:00+00:00", state_manager=state_manager, session="session"
        )

    def test_scheduled_becomes_in_progress_atomically(self, clean_dag, services, state_manager):
        services.bucketService.transition_bucket_clean.return_value = True

        assert self.run(clean_dag, state_manager) is True

        services.bucketService.transition_bucket_clean.assert_called_once_with(
            "sub-1", CleanStatus.SCHEDULED, CleanStatus.INPROGRESS, "session"
        )
        state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.INPROGRESS.value})

    def test_cancelled_meanwhile_declines_without_deleting_anything(self, clean_dag, services, state_manager):
        services.bucketService.transition_bucket_clean.return_value = False
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_status=CleanStatus.CANCELLED.value)

        with pytest.raises(DeclineDemandException, match="cancelled during the grace period.*status: cancelled.*nothing was deleted"):
            self.run(clean_dag, state_manager)

        state_manager.push_state.assert_not_called()
        services.bucketService.update_bucket_clean_status.assert_not_called()


class TestGetCosApiKey:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["get_cos_api_key"](
            bucket=bucket_row(), claimed=True, session="session", state_manager=state_manager, reader="reader", vault="vault"
        )

    def test_reads_the_key_through_vault(self, clean_dag, services, state_manager):
        services.vault_service.get_cos_api_key.return_value = "api-key"

        assert self.run(clean_dag, state_manager) == "api-key"

        services.vault_service.get_cos_api_key.assert_called_once_with(bucket_row(), "vault", "reader")
        state_manager.push_state.assert_not_called()

    def test_vault_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, state_manager):
        services.vault_service.get_cos_api_key.side_effect = RuntimeError("vault down")

        with pytest.raises(RuntimeError, match="vault down"):
            self.run(clean_dag, state_manager)

        assert_failed(services, state_manager)


class TestIsBucketEmpty:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["is_bucket_empty"](
            bucket=bucket_row(), api_key="api-key", state_manager=state_manager, session="session"
        )

    @pytest.mark.parametrize("has_contents, expected", [(False, True), (True, False)])
    def test_answers_from_the_bucket_listing(self, clean_dag, s3, state_manager, has_contents, expected):
        s3.bucketService.check_bucket_has_contents.return_value = has_contents

        assert self.run(clean_dag, state_manager) is expected

        s3.ibm_iam_service.get_iam_access_token.assert_called_once_with("api-key")
        s3.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())

    def test_listing_failure_marks_the_clean_failed_and_reraises(self, clean_dag, s3, state_manager):
        s3.bucketService.check_bucket_has_contents.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager)

        assert_failed(s3, state_manager)


class TestCreateExpirationRule:
    def run(self, clean_dag, state_manager, is_bucket_empty):
        return clean_dag.steps["create_expiration_rule"](
            api_key="api-key", bucket=bucket_row(), is_bucket_empty=is_bucket_empty,
            state_manager=state_manager, session="session",
        )

    def test_non_empty_bucket_gets_the_rule(self, clean_dag, s3, state_manager):
        s3.bucketService.create_expiration_rule.return_value = True

        assert self.run(clean_dag, state_manager, is_bucket_empty=False) is True

        s3.bucketService.create_expiration_rule.assert_called_once_with("tok", bucket_row())

    def test_empty_bucket_needs_no_rule(self, clean_dag, s3, state_manager):
        assert self.run(clean_dag, state_manager, is_bucket_empty=True) is False

        s3.bucketService.create_expiration_rule.assert_not_called()
        s3.ibm_iam_service.get_iam_access_token.assert_not_called()

    def test_failure_marks_the_clean_failed_and_reraises(self, clean_dag, s3, state_manager):
        s3.bucketService.create_expiration_rule.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager, is_bucket_empty=False)

        assert_failed(s3, state_manager)


class TestSaveCreateExpirationRuleInDb:
    def run(self, clean_dag, payload, created, state_manager=None):
        return clean_dag.steps["save_create_expiration_rule_in_db"](
            is_expiration_created=created, bucket=bucket_row(), payload=payload,
            state_manager=state_manager, session="session",
        )

    def test_rule_created_is_recorded_after_disabling_the_previous_ones(self, clean_dag, services, payload):
        assert self.run(clean_dag, payload, created=True) is True  # drapeau rendu tel quel, pour le sensor

        lifecycle = services.lifecyclePolicyRuleService
        lifecycle.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_called_once_with(bucket_row(), "karim", "session")
        # Règle "clean_bucket" sur tout le bucket (préfixe vide) : objets courants et
        # versions non courantes expirent après 1 jour.
        lifecycle.complete_lifecycle_policy_rule_creation.assert_called_once_with(
            bucket_row(), "clean_bucket", "", 1, 1, "karim", "session"
        )

    def test_nothing_recorded_without_a_rule(self, clean_dag, services, payload):
        assert self.run(clean_dag, payload, created=False) is False

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_not_called()
        services.lifecyclePolicyRuleService.complete_lifecycle_policy_rule_creation.assert_not_called()

    def test_db_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, payload, state_manager):
        services.lifecyclePolicyRuleService.complete_lifecycle_policy_rule_creation.side_effect = RuntimeError("db")

        with pytest.raises(RuntimeError, match="db"):
            self.run(clean_dag, payload, created=True, state_manager=state_manager)

        assert_failed(services, state_manager)


class TestSchedulerCleanBucket:
    def run(self, clean_dag, state_manager, created):
        return clean_dag.steps["scheduler_clean_bucket"](
            bucket=bucket_row(), api_key="api-key", is_expiration_created=created,
            state_manager=state_manager, session="session",
        )

    @pytest.mark.parametrize("has_contents, is_done", [(True, False), (False, True)])
    def test_pokes_until_the_bucket_is_empty(self, clean_dag, s3, state_manager, has_contents, is_done):
        s3.bucketService.check_bucket_has_contents.return_value = has_contents

        result = self.run(clean_dag, state_manager, created=True)

        assert isinstance(result, PokeReturnValue)
        assert (result.is_done, result.xcom_value) == (is_done, {"content": "clean"})
        s3.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())

    def test_without_a_rule_the_sensor_is_done_at_once(self, clean_dag, s3, state_manager):
        result = self.run(clean_dag, state_manager, created=False)

        assert (result.is_done, result.xcom_value) == (True, {"content": "clean"})
        s3.bucketService.check_bucket_has_contents.assert_not_called()

    def test_listing_failure_marks_the_clean_failed_and_reraises(self, clean_dag, s3, state_manager):
        s3.bucketService.check_bucket_has_contents.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager, created=True)

        assert_failed(s3, state_manager)


class TestDeleteExpirationRule:
    DONE = PokeReturnValue(is_done=True, xcom_value={"content": "clean"})

    def run(self, clean_dag, state_manager, created, check_clean_done=DONE):
        return clean_dag.steps["delete_expiration_rule"](
            bucket=bucket_row(), api_key="api-key", is_expiration_created=created, check_clean_done=check_clean_done,
            state_manager=state_manager, session="session",
        )

    def test_rule_removed_without_touching_the_status_yet(self, clean_dag, s3, state_manager):
        assert self.run(clean_dag, state_manager, created=True) is True

        s3.bucketService.delete_lifecycle_policy.assert_called_once_with("tok", bucket_row())
        s3.bucketService.update_bucket_clean_status.assert_not_called()  # success posé par complete_clean

    def test_bucket_already_empty_keeps_its_lifecycle_configuration(self, clean_dag, s3, state_manager):
        assert self.run(clean_dag, state_manager, created=False) is False

        s3.bucketService.delete_lifecycle_policy.assert_not_called()
        s3.ibm_iam_service.get_iam_access_token.assert_not_called()

    @pytest.mark.parametrize("check_clean_done", [DONE, {"content": "clean"}, None])
    def test_the_sensor_result_only_orders_the_steps(self, clean_dag, s3, state_manager, check_clean_done):
        assert self.run(clean_dag, state_manager, created=True, check_clean_done=check_clean_done) is True

    def test_missing_policy_counts_as_removed(self, clean_dag, s3, state_manager):
        s3.bucketService.delete_lifecycle_policy.side_effect = S3Error(404)

        assert self.run(clean_dag, state_manager, created=True) is True
        state_manager.push_state.assert_not_called()

    def test_other_s3_error_marks_the_clean_failed_and_reraises(self, clean_dag, s3, state_manager):
        s3.bucketService.delete_lifecycle_policy.side_effect = S3Error(500)

        with pytest.raises(S3Error):
            self.run(clean_dag, state_manager, created=True)

        assert_failed(s3, state_manager)

    def test_error_without_code_is_a_failure_too(self, clean_dag, s3, state_manager):
        s3.ibm_iam_service.get_iam_access_token.side_effect = RuntimeError("iam down")

        with pytest.raises(RuntimeError, match="iam down"):
            self.run(clean_dag, state_manager, created=True)

        assert_failed(s3, state_manager)


class TestSaveDeleteExpirationRuleInDb:
    def run(self, clean_dag, payload, deleted, state_manager=None):
        return clean_dag.steps["save_delete_expiration_rule_in_db"](
            is_expiration_deleted=deleted, bucket=bucket_row(), payload=payload,
            state_manager=state_manager, session="session",
        )

    def test_rules_disabled_once_the_policy_is_gone(self, clean_dag, services, payload):
        assert self.run(clean_dag, payload, deleted=True) is True

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_called_once_with(
            bucket_row(), "karim", "session"
        )

    def test_nothing_disabled_without_a_removed_rule(self, clean_dag, services, payload):
        assert self.run(clean_dag, payload, deleted=False) is True

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_not_called()

    def test_db_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, payload, state_manager):
        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.side_effect = RuntimeError("db")

        with pytest.raises(RuntimeError, match="db"):
            self.run(clean_dag, payload, deleted=True, state_manager=state_manager)

        assert_failed(services, state_manager)


class TestCompleteClean:
    def test_marks_the_clean_a_success_once_the_rule_is_gone(self, clean_dag, services, state_manager):
        assert clean_dag.steps["complete_clean"](bucket=bucket_row(), rules_saved=True, state_manager=state_manager, session="session") is True

        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", CleanStatus.SUCCESS, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.SUCCESS.value})
