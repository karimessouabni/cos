"""Tests des étapes du DAG ``cos.bucket.v1.cancel_clean``."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.clean_status import CleanStatus


def in_days(days: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


EXECUTE_AT = in_days(5)  # une seule valeur par session de test : les lignes se comparent entre elles


def bucket_row(**overrides) -> dict:
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "clean_status": CleanStatus.SCHEDULED.value,
        "clean_execute_at": EXECUTE_AT,
        "workspace": {"workspace_id": "ws-1"},
    }
    row.update(overrides)
    return row


@pytest.fixture
def cancel_dag(load_dag):
    return load_dag("cos.bucket.v1.cancel_clean.py")


@pytest.fixture
def payload(cancel_dag):
    return cancel_dag.module.BucketCancelCleanPayload(subscription_id="sub-1", requestor="karim")


@pytest.fixture
def tf():
    return MagicMock(name="tf")


def test_dag_identity(cancel_dag):
    assert cancel_dag.module.bucket_cancel_clean.dag_name == "cos.bucket.v1.cancel_clean"
    assert cancel_dag.module.bucket_cancel_clean.config.options == {"lock_subscription_on_failure": False}
    assert list(cancel_dag.steps) == ["validate_bucket", "cancel_clean", "lift_quarantine"]


class TestValidateBucket:
    def run(self, cancel_dag, payload):
        return cancel_dag.steps["validate_bucket"](session="session", payload=payload)

    def test_a_scheduled_clean_in_its_grace_period_can_be_cancelled(self, cancel_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert self.run(cancel_dag, payload) == bucket_row()

    @pytest.mark.parametrize("status", [CleanStatus.FAILED.value, CleanStatus.CANCELLED.value])
    def test_a_failed_or_cancelled_clean_can_be_cancelled_again_to_lift_the_quarantine(self, cancel_dag, services, payload, status):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_status=status, clean_execute_at=in_days(-3))

        assert self.run(cancel_dag, payload)["clean_status"] == status

    @pytest.mark.parametrize("row", [None, bucket_row(name="")])
    def test_missing_bucket_is_declined(self, cancel_dag, services, payload, row):
        services.bucketService.get_bucket_by_sub_id.return_value = row

        with pytest.raises(DeclineDemandException, match="doesn't exist or not fully created"):
            self.run(cancel_dag, payload)

    @pytest.mark.parametrize("status", [CleanStatus.INPROGRESS.value, CleanStatus.SUCCESS.value, None])
    def test_nothing_to_cancel_is_declined(self, cancel_dag, services, payload, status):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_status=status)

        with pytest.raises(DeclineDemandException, match=f"no clean to cancel.*clean status: {status}"):
            self.run(cancel_dag, payload)

    @pytest.mark.parametrize("execute_at", [in_days(-0.01), datetime.now(timezone.utc) - timedelta(hours=1), "2020-01-01T00:00:00"])
    def test_grace_period_over_is_declined(self, cancel_dag, services, payload, execute_at):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_execute_at=execute_at)

        with pytest.raises(DeclineDemandException, match="grace period .* ended at .*cannot be cancelled any more"):
            self.run(cancel_dag, payload)


class TestCancelClean:
    def run(self, cancel_dag, state_manager, status=CleanStatus.SCHEDULED.value):
        return cancel_dag.steps["cancel_clean"](
            bucket=bucket_row(clean_status=status), state_manager=state_manager, session="session"
        )

    @pytest.mark.parametrize("status", [CleanStatus.SCHEDULED, CleanStatus.FAILED, CleanStatus.CANCELLED])
    def test_transition_is_conditional_on_the_current_status(self, cancel_dag, services, state_manager, status):
        services.bucketService.transition_bucket_clean.return_value = True

        assert self.run(cancel_dag, state_manager, status.value) is True

        services.bucketService.transition_bucket_clean.assert_called_once_with(
            "sub-1", status, CleanStatus.CANCELLED, "session"
        )
        state_manager.push_state.assert_called_once_with({"clean_status": CleanStatus.CANCELLED.value})

    def test_clean_that_just_started_cannot_be_cancelled(self, cancel_dag, services, state_manager):
        services.bucketService.transition_bucket_clean.return_value = False

        with pytest.raises(DeclineDemandException, match="just started, it cannot be cancelled"):
            self.run(cancel_dag, state_manager)

        state_manager.push_state.assert_not_called()


class TestLiftQuarantine:
    def run(self, cancel_dag, payload, tf, state_manager):
        return cancel_dag.steps["lift_quarantine"](
            bucket=bucket_row(), cancelled=True, payload=payload, tf=tf, vault="vault", reader="reader",
            state_manager=state_manager, session="session",
        )

    def test_lifts_the_quarantine_through_the_workspace(self, cancel_dag, services, payload, tf, state_manager):
        assert self.run(cancel_dag, payload, tf, state_manager) is True

        services.quarantine_service.set_bucket_quarantine.assert_called_once_with(
            bucket=bucket_row(), enabled=False, payload=payload, description="my bucket",
            tf=tf, vault="vault", reader="reader", session="session",
        )
        state_manager.push_state.assert_called_once_with({"quarantine": False})

    def test_apply_failure_keeps_the_cancelled_status_and_reraises(self, cancel_dag, services, payload, tf, state_manager):
        services.quarantine_service.set_bucket_quarantine.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(cancel_dag, payload, tf, state_manager)

        services.bucketService.update_bucket_clean_status.assert_not_called()
        state_manager.push_state.assert_not_called()
