"""Tests du DAG ``cos.bucket.v1.quarantine_test`` : pose la règle, attend le 403
côté Airflow, vérifie le 200 côté Schematics, lève, attend le 200, compte rendu
dans le state."""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.clean_status import CleanStatus


def bucket_row(**overrides) -> dict:
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "clean_status": CleanStatus.SUCCESS.value,
        "clean_cbr_workspace_id": None,
        "virtual_server_endpoint": "https://vpe/bucket-a",
        "cos": {"crn": "crn:cos"},
    }
    row.update(overrides)
    return row


@pytest.fixture
def qt_dag(load_dag):
    return load_dag("cos.bucket.v1.quarantine_test.py")


@pytest.fixture
def payload(qt_dag):
    return qt_dag.module.BucketQuarantineTestPayload(subscription_id="sub-1", requestor="karim")


@pytest.fixture
def tf():
    return MagicMock(name="tf")


@pytest.fixture
def s3(services):
    services.ibm_iam_service.get_iam_access_token.return_value = "tok"
    return services


def test_dag_identity(qt_dag):
    assert qt_dag.module.bucket_quarantine_test.dag_name == "cos.bucket.v1.quarantine_test"
    assert qt_dag.module.bucket_quarantine_test.config.options == {"lock_subscription_on_failure": False}
    assert list(qt_dag.steps) == [
        "validate_bucket", "get_cos_api_key", "check_access_before", "set_quarantine",
        "wait_until_blocked", "check_schematics_access", "lift_quarantine", "wait_until_restored",
    ]


class TestProbeUntil:
    def test_stops_at_the_expected_status_and_reports_the_delay(self, qt_dag, s3):
        s3.bucketService.bucket_access_status.side_effect = [200, 200, 403]
        sleep = MagicMock()

        result = qt_dag.module.probe_until("tok", bucket_row(), 403, sleep=sleep, interval=30, attempts=5)

        assert result == {"reached": True, "status": 403, "attempts": 3, "seconds": 60}
        assert sleep.call_count == 2

    def test_gives_up_after_the_attempts_without_raising(self, qt_dag, s3):
        s3.bucketService.bucket_access_status.side_effect = [200, 500, 200]
        sleep = MagicMock()

        result = qt_dag.module.probe_until("tok", bucket_row(), 403, sleep=sleep, interval=30, attempts=3)

        assert result == {"reached": False, "status": 200, "attempts": 3, "seconds": 60, "statuses_seen": [200, 500]}
        assert sleep.call_count == 2  # pas d'attente après le dernier essai

    def test_defaults_bound_the_wait_to_twenty_minutes(self, qt_dag):
        assert qt_dag.module.PROBE_INTERVAL_SECONDS * (qt_dag.module.PROBE_MAX_ATTEMPTS - 1) == 19.5 * 60


class TestValidateBucket:
    def run(self, qt_dag, payload):
        return qt_dag.steps["validate_bucket"](session="session", payload=payload)

    def test_accepts_a_quiet_bucket(self, qt_dag, services, payload):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert self.run(qt_dag, payload) == bucket_row()

    @pytest.mark.parametrize("row, reason", [
        (None, "doesn't exist"),
        (bucket_row(virtual_server_endpoint=None), "no endpoint"),
        (bucket_row(clean_status=CleanStatus.SCHEDULED.value), "already owns the quarantine"),
        (bucket_row(clean_cbr_workspace_id="ws-cbr-old"), "still has a quarantine workspace"),
    ])
    def test_declines_what_would_make_the_test_meaningless(self, qt_dag, services, payload, row, reason):
        services.bucketService.get_bucket_by_sub_id.return_value = row

        with pytest.raises(DeclineDemandException, match=reason):
            self.run(qt_dag, payload)


class TestCheckAccessBefore:
    def test_requires_a_200_before_the_quarantine(self, qt_dag, s3):
        s3.bucketService.bucket_access_status.return_value = 200

        assert qt_dag.steps["check_access_before"](bucket=bucket_row(), api_key="api-key") == 200

        s3.bucketService.bucket_access_status.assert_called_once_with("tok", bucket_row())

    def test_any_other_status_declines(self, qt_dag, s3):
        s3.bucketService.bucket_access_status.return_value = 403

        with pytest.raises(DeclineDemandException, match="HTTP 403 before any quarantine"):
            qt_dag.steps["check_access_before"](bucket=bucket_row(), api_key="api-key")


class TestSetQuarantine:
    def test_sets_the_rule_records_the_workspace_and_reports_the_mode(self, qt_dag, services, payload, tf, state_manager):
        services.quarantine_service.set_bucket_quarantine.return_value = "ws-cbr-1"
        services.quarantine_service.quarantine_settings.return_value = {"enforcement_mode": "report"}

        result = qt_dag.steps["set_quarantine"](
            bucket=bucket_row(), access_before=200, payload=payload, tf=tf, vault="vault", reader="reader",
            state_manager=state_manager, session="session",
        )

        assert result == "ws-cbr-1"
        services.quarantine_service.set_bucket_quarantine.assert_called_once_with(
            tf=tf, bucket=bucket_row(), payload=payload, vault="vault", reader="reader"
        )
        services.bucketService.set_bucket_clean_workspace.assert_called_once_with("sub-1", "ws-cbr-1", "session")
        state_manager.push_state.assert_called_once_with(
            {"quarantine_test": {"workspace_id": "ws-cbr-1", "enforcement_mode": "report"}}
        )


class TestWaitUntilBlocked:
    def test_reports_without_raising_even_when_never_blocked(self, qt_dag, s3, monkeypatch):
        monkeypatch.setattr(qt_dag.module, "PROBE_MAX_ATTEMPTS", 2)
        monkeypatch.setattr(qt_dag.module.time, "sleep", lambda s: None)
        s3.bucketService.bucket_access_status.return_value = 200

        result = qt_dag.steps["wait_until_blocked"](bucket=bucket_row(), api_key="api-key", workspace_id="ws-cbr-1")

        assert result["reached"] is False and result["status"] == 200


SCHEMATICS_OK = {"reached": True, "status": 200, "empty": True}


class TestCheckSchematicsAccess:
    def run(self, qt_dag, tf):
        return qt_dag.steps["check_schematics_access"](
            bucket=bucket_row(), blocked={"reached": True}, workspace_id="ws-cbr-1", tf=tf, vault="vault", reader="reader"
        )

    def test_schematics_lists_the_bucket_while_airflow_is_blocked(self, qt_dag, services, tf):
        services.quarantine_service.probe_bucket_via_schematics.return_value = {"status": 200, "empty": True}

        assert self.run(qt_dag, tf) == SCHEMATICS_OK
        services.quarantine_service.probe_bucket_via_schematics.assert_called_once_with(
            tf=tf, bucket=bucket_row(), workspace_id="ws-cbr-1", vault="vault", reader="reader"
        )

    def test_blocked_schematics_is_reported_not_raised(self, qt_dag, services, tf):
        services.quarantine_service.probe_bucket_via_schematics.return_value = {"status": 403, "empty": None}

        result = self.run(qt_dag, tf)

        assert result == {"reached": False, "status": 403, "empty": None}

    def test_a_failing_workspace_never_prevents_the_lift(self, qt_dag, services, tf):
        services.quarantine_service.probe_bucket_via_schematics.side_effect = RuntimeError("apply failed")

        result = self.run(qt_dag, tf)

        assert result["reached"] is False and result["error"] == "apply failed"


class TestLiftQuarantine:
    def test_lifts_the_workspace_recorded_in_db(self, qt_dag, services, tf):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_cbr_workspace_id="ws-cbr-1")

        assert qt_dag.steps["lift_quarantine"](bucket=bucket_row(), schematics=SCHEMATICS_OK, tf=tf, session="session") is True

        services.quarantine_service.lift_bucket_quarantine.assert_called_once_with(tf=tf, workspace_id="ws-cbr-1")
        services.bucketService.set_bucket_clean_workspace.assert_called_once_with("sub-1", None, "session")

    def test_nothing_to_lift_without_a_workspace(self, qt_dag, services, tf):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert qt_dag.steps["lift_quarantine"](bucket=bucket_row(), schematics=SCHEMATICS_OK, tf=tf, session="session") is True

        services.quarantine_service.lift_bucket_quarantine.assert_not_called()


class TestWaitUntilRestored:
    def run(self, qt_dag, state_manager, blocked, schematics=SCHEMATICS_OK):
        return qt_dag.steps["wait_until_restored"](
            bucket=bucket_row(), api_key="api-key", lifted=True, blocked=blocked, schematics=schematics,
            state_manager=state_manager,
        )

    def test_full_report_in_the_state_when_everything_went_well(self, qt_dag, s3, state_manager, monkeypatch):
        monkeypatch.setattr(qt_dag.module.time, "sleep", lambda s: None)
        s3.bucketService.bucket_access_status.side_effect = [403, 200]
        blocked = {"reached": True, "status": 403, "attempts": 4, "seconds": 90}

        report = self.run(qt_dag, state_manager, blocked)

        assert report["verdict"] == "ok"
        assert report["schematics"] == SCHEMATICS_OK
        assert report["restored"] == {"reached": True, "status": 200, "attempts": 2, "seconds": 30}
        state_manager.push_state.assert_called_once_with({"quarantine_test": report})

    def test_schematics_blocked_gives_ko(self, qt_dag, s3, state_manager):
        s3.bucketService.bucket_access_status.return_value = 200
        blocked = {"reached": True, "status": 403, "attempts": 4, "seconds": 90}

        report = self.run(qt_dag, state_manager, blocked, {"reached": False, "status": 403, "empty": None})

        assert report["verdict"] == "ko"

    def test_never_blocked_gives_ko_but_the_lift_is_verified(self, qt_dag, s3, state_manager):
        s3.bucketService.bucket_access_status.return_value = 200

        report = self.run(qt_dag, state_manager, {"reached": False, "status": 200, "attempts": 40, "seconds": 1170})

        assert report["verdict"] == "ko" and report["restored"]["reached"] is True

    def test_access_not_restored_is_a_failure(self, qt_dag, s3, state_manager, monkeypatch):
        monkeypatch.setattr(qt_dag.module, "PROBE_MAX_ATTEMPTS", 1)
        s3.bucketService.bucket_access_status.return_value = 403

        with pytest.raises(RuntimeError, match="still answers HTTP 403 after the quarantine was lifted"):
            self.run(qt_dag, state_manager, {"reached": True, "status": 403, "attempts": 1, "seconds": 0})

        state_manager.push_state.assert_called_once()  # le compte rendu est quand même écrit
