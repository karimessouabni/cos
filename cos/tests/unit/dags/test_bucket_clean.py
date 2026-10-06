"""Tests des étapes du DAG ``cos.bucket.v1.clean`` (période de grâce + quarantaine)."""
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
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "workspace": {"workspace_id": "ws-1"},
        "cos": {"crn": "crn:cos"},
    }
    row.update(overrides)
    return row


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


@pytest.fixture
def tf():
    from unittest.mock import MagicMock

    return MagicMock(name="tf")


def assert_failed(services, state_manager):
    services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", CleanStatus.FAILED, "session")
    state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.FAILED.value})


def test_dag_identity(clean_dag):
    assert clean_dag.module.bucket_clean.dag_name == "cos.bucket.v1.clean"
    assert clean_dag.module.bucket_clean.config.options == {"lock_subscription_on_failure": False}
    assert list(clean_dag.steps) == [
        "validate_bucket",
        "schedule_clean",
        "quarantine_bucket",
        "claim_clean",
        "get_cos_api_key",
        "is_bucket_empty",
        "create_expiration_rule",
        "save_create_expiration_rule_in_db",
        "scheduler_clean_bucket",
        "delete_expiration_rule",
        "save_delete_expiration_rule_in_db",
        "lift_quarantine",
    ]


def test_wiring_waits_for_the_grace_period_between_quarantine_and_claim(clean_dag):
    """Le sensor de date est câblé entre la quarantaine et la décision ; il lit la
    date renvoyée par schedule_clean. Airflow ne déduit l'ordre que des valeurs
    consommées : chaque étape de base est consommée par la suivante."""
    import inspect

    source = inspect.getsource(clean_dag.module)
    assert 'task_id="wait_for_grace_period"' in source
    assert "xcom_pull(task_ids='%s') }}\" % execute_at.operator.task_id" in source
    assert "quarantined >> wait" in source and "wait >> claimed" in source
    assert "claim_clean(bucket=bucket, quarantined=quarantined)" in source
    assert "get_cos_api_key(bucket=bucket, claimed=claimed)" in source
    # Un step.sensor refuse un mot-clé supplémentaire : il consomme le drapeau renvoyé par la sauvegarde.
    assert "scheduler_clean_bucket(bucket=bucket, api_key=api_key, is_expiration_created=rule_saved)" in source
    assert "rule_saved=" not in source.split("def bucket_clean")[1].split("scheduler_clean_bucket(bucket=")[0]
    assert "lift_quarantine(bucket=bucket, rules_saved=rules_saved)" in source


def test_only_the_scheduler_is_a_sensor_with_a_bounded_wait(clean_dag):
    assert clean_dag.steps["scheduler_clean_bucket"].sensor_options == {
        "exponential_backoff": False, "poke_interval": 3 * 3600, "timeout": 7 * 24 * 3600, "mode": "reschedule",
    }
    assert all(clean_dag.steps[name].sensor_options is None for name in clean_dag.steps if name != "scheduler_clean_bucket")


class TestGracePeriod:
    def test_defaults_to_seven_days(self, clean_dag, monkeypatch):
        monkeypatch.delenv("COS_CLEAN_GRACE_DAYS", raising=False)

        assert clean_dag.module.grace_period() == timedelta(days=7)

    def test_can_be_shortened_for_the_toolchain_tests(self, clean_dag, monkeypatch):
        monkeypatch.setenv("COS_CLEAN_GRACE_DAYS", "0")

        assert clean_dag.module.grace_period() == timedelta(0)


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
        {"retention_enabled": True},
        {"object_lock_duration_days": 30},
        {"object_lock_duration_years": 1},
    ])
    def test_locked_objects_cannot_expire_so_the_clean_is_declined_with_a_way_out(self, clean_dag, services, payload, locked):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(**locked)

        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(clean_dag, payload)

        message = str(excinfo.value)
        assert "retention policy or an object lock" in message
        assert "Delete the unlocked objects yourself" in message
        assert "once every retention has expired" in message

    @pytest.mark.parametrize("workspace", [None, {"workspace_id": None}])
    def test_no_workspace_means_no_quarantine_so_declined(self, clean_dag, services, payload, workspace):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(workspace=workspace)

        with pytest.raises(DeclineDemandException, match="no Terraform workspace"):
            self.run(clean_dag, payload)


class TestScheduleClean:
    def test_marks_scheduled_with_the_execution_date_and_returns_it(self, clean_dag, services, state_manager):
        # La grâce est lue par schematics_service.setting (Airflow Variable, env, défaut) : ici doublé.
        services.schematics_service.setting.return_value = "7"
        before = datetime.now(timezone.utc)

        execute_at = clean_dag.steps["schedule_clean"](bucket=bucket_row(), state_manager=state_manager, session="session")

        args = services.bucketService.schedule_bucket_clean.call_args.args
        assert args[0] == "sub-1" and args[3] == "session"
        requested_at, planned = args[1], args[2]
        assert before <= requested_at <= datetime.now(timezone.utc)
        assert planned - requested_at == timedelta(days=7)
        assert planned.tzinfo is not None
        assert execute_at == planned.isoformat()
        state_manager.push_state.assert_called_once_with({
            "clean_status": CleanStatus.SCHEDULED.value,
            "clean_requested_at": requested_at.isoformat(),
            "clean_execute_at": planned.isoformat(),
        })
        services.schematics_service.setting.assert_called_once_with("cos_clean_grace_days", "7")


class TestQuarantineBucket:
    def run(self, clean_dag, payload, tf, state_manager):
        return clean_dag.steps["quarantine_bucket"](
            bucket=bucket_row(), execute_at="2026-10-13T10:00:00+00:00", payload=payload, tf=tf,
            vault="vault", reader="reader", state_manager=state_manager, session="session",
        )

    def test_sets_the_quarantine_through_the_workspace(self, clean_dag, services, payload, tf, state_manager):
        assert self.run(clean_dag, payload, tf, state_manager) is True

        services.quarantine_service.set_bucket_quarantine.assert_called_once_with(
            bucket=bucket_row(), enabled=True, payload=payload, description="my bucket",
            tf=tf, vault="vault", reader="reader", session="session",
        )
        state_manager.push_state.assert_called_once_with({"quarantine": True})

    def test_apply_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, payload, tf, state_manager):
        services.quarantine_service.set_bucket_quarantine.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(clean_dag, payload, tf, state_manager)

        assert_failed(services, state_manager)


class TestClaimClean:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["claim_clean"](bucket=bucket_row(), quarantined=True, state_manager=state_manager, session="session")

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
        s3.bucketService.update_bucket_clean_status.assert_not_called()  # success après la levée de quarantaine

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


class TestLiftQuarantine:
    def run(self, clean_dag, payload, tf, state_manager):
        return clean_dag.steps["lift_quarantine"](
            bucket=bucket_row(), rules_saved=True, payload=payload, tf=tf, vault="vault", reader="reader",
            state_manager=state_manager, session="session",
        )

    def test_lifts_the_quarantine_then_the_clean_is_a_success(self, clean_dag, services, payload, tf, state_manager):
        assert self.run(clean_dag, payload, tf, state_manager) is True

        services.quarantine_service.set_bucket_quarantine.assert_called_once_with(
            bucket=bucket_row(), enabled=False, payload=payload, description="my bucket",
            tf=tf, vault="vault", reader="reader", session="session",
        )
        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", CleanStatus.SUCCESS, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.SUCCESS.value, "quarantine": False})

    def test_apply_failure_leaves_the_bucket_quarantined_and_failed(self, clean_dag, services, payload, tf, state_manager):
        """cancel_clean (accepté sur failed) lèvera la quarantaine."""
        services.quarantine_service.set_bucket_quarantine.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(clean_dag, payload, tf, state_manager)

        assert_failed(services, state_manager)
