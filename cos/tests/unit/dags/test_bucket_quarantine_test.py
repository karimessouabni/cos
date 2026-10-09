"""Tests du DAG ``cos.bucket.v1.quarantine_test`` : éprouve le cycle du clean
(fermer, ouvrir, refermer, lever) et l'API de configuration pendant le blocage,
compte rendu dans le state."""
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
        "wait_until_blocked", "check_config_api", "reopen_quarantine", "reclose_quarantine",
        "lift_quarantine", "wait_until_restored",
    ]


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


REACHED_403 = {"reached": True, "status": 403, "attempts": 4, "seconds": 90}
REACHED_200 = {"reached": True, "status": 200, "attempts": 3, "seconds": 60}


class TestWaitUntilBlocked:
    def test_reports_without_raising_even_when_never_blocked(self, qt_dag, s3):
        s3.quarantine_service.probe_bucket_until.return_value = {"reached": False, "status": 200, "attempts": 40, "seconds": 1170}

        result = qt_dag.steps["wait_until_blocked"](bucket=bucket_row(), api_key="api-key", workspace_id="ws-cbr-1")

        assert result["reached"] is False and result["status"] == 200
        s3.quarantine_service.probe_bucket_until.assert_called_once_with("tok", bucket_row(), 403)


class TestCheckConfigApi:
    def run(self, qt_dag):
        return qt_dag.steps["check_config_api"](bucket=bucket_row(), api_key="api-key", blocked=REACHED_403)

    def test_reports_whether_the_configuration_api_passes_the_rule(self, qt_dag, s3):
        s3.schematics_service.setting.side_effect = lambda name, default: default
        s3.bucketService.COS_CONFIG_API = "https://config.direct.example/v1"
        s3.bucketService.bucket_config_metadata.return_value = {"status": 200, "object_count": 3}

        assert self.run(qt_dag) == {"endpoint": "https://config.direct.example/v1", "status": 200, "object_count": 3}
        s3.bucketService.bucket_config_metadata.assert_called_once_with("tok", bucket_row(), "https://config.direct.example/v1")

    def test_an_unreachable_api_is_reported_not_raised(self, qt_dag, s3):
        s3.schematics_service.setting.side_effect = lambda name, default: "https://config.private.example/v1"
        s3.bucketService.bucket_config_metadata.side_effect = RuntimeError("timeout")

        assert self.run(qt_dag) == {"endpoint": "https://config.private.example/v1", "status": None, "error": "timeout"}


class TestReopenAndReclose:
    def reopen(self, qt_dag, tf):
        return qt_dag.steps["reopen_quarantine"](
            bucket=bucket_row(), api_key="api-key", workspace_id="ws-cbr-1", config_api={"status": 403},
            tf=tf, vault="vault", reader="reader",
        )

    def reclose(self, qt_dag, tf):
        return qt_dag.steps["reclose_quarantine"](
            bucket=bucket_row(), api_key="api-key", workspace_id="ws-cbr-1", reopened=REACHED_200,
            tf=tf, vault="vault", reader="reader",
        )

    def test_reopen_uses_the_clean_opening(self, qt_dag, s3, tf):
        s3.quarantine_service.open_bucket_quarantine.return_value = REACHED_200

        assert self.reopen(qt_dag, tf) == REACHED_200
        s3.quarantine_service.open_bucket_quarantine.assert_called_once_with(
            tf=tf, bucket=bucket_row(), workspace_id="ws-cbr-1", vault="vault", reader="reader", access_token="tok"
        )

    def test_a_failed_reopen_is_reported_not_raised(self, qt_dag, s3, tf):
        s3.quarantine_service.open_bucket_quarantine.side_effect = RuntimeError("still answers HTTP 403")

        assert self.reopen(qt_dag, tf) == {"reached": False, "status": None, "error": "still answers HTTP 403"}

    def test_reclose_enables_the_rule_and_waits_for_the_403(self, qt_dag, s3, tf):
        s3.quarantine_service.probe_bucket_until.return_value = REACHED_403

        assert self.reclose(qt_dag, tf) == REACHED_403
        s3.quarantine_service.close_bucket_quarantine.assert_called_once_with(
            tf=tf, bucket=bucket_row(), workspace_id="ws-cbr-1", vault="vault", reader="reader"
        )
        s3.quarantine_service.probe_bucket_until.assert_called_once_with("tok", bucket_row(), 403)

    def test_a_failed_reclose_is_reported_not_raised(self, qt_dag, s3, tf):
        s3.quarantine_service.close_bucket_quarantine.side_effect = RuntimeError("apply failed")

        assert self.reclose(qt_dag, tf) == {"reached": False, "status": None, "error": "apply failed"}
        s3.quarantine_service.probe_bucket_until.assert_not_called()


class TestLiftQuarantine:
    def test_lifts_the_workspace_recorded_in_db(self, qt_dag, services, tf):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_cbr_workspace_id="ws-cbr-1")

        assert qt_dag.steps["lift_quarantine"](bucket=bucket_row(), reclosed=REACHED_403, tf=tf, session="session") is True

        services.quarantine_service.lift_bucket_quarantine.assert_called_once_with(tf=tf, workspace_id="ws-cbr-1")
        services.bucketService.set_bucket_clean_workspace.assert_called_once_with("sub-1", None, "session")

    def test_nothing_to_lift_without_a_workspace(self, qt_dag, services, tf):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert qt_dag.steps["lift_quarantine"](bucket=bucket_row(), reclosed=REACHED_403, tf=tf, session="session") is True

        services.quarantine_service.lift_bucket_quarantine.assert_not_called()


class TestWaitUntilRestored:
    CONFIG = {"endpoint": "https://config/v1", "status": 403}

    def run(self, qt_dag, state_manager, blocked=REACHED_403, reopened=REACHED_200, reclosed=REACHED_403):
        return qt_dag.steps["wait_until_restored"](
            bucket=bucket_row(), api_key="api-key", lifted=True, blocked=blocked, config_api=self.CONFIG,
            reopened=reopened, reclosed=reclosed, state_manager=state_manager,
        )

    def test_full_report_in_the_state_when_every_transition_happened(self, qt_dag, s3, state_manager):
        s3.quarantine_service.probe_bucket_until.return_value = REACHED_200

        report = self.run(qt_dag, state_manager)

        assert report == {"blocked": REACHED_403, "config_api": self.CONFIG, "reopened": REACHED_200,
                          "reclosed": REACHED_403, "restored": REACHED_200, "verdict": "ok"}
        state_manager.push_state.assert_called_once_with({"quarantine_test": report})

    @pytest.mark.parametrize("missed", ["blocked", "reopened", "reclosed"])
    def test_any_missed_transition_gives_ko(self, qt_dag, s3, state_manager, missed):
        s3.quarantine_service.probe_bucket_until.return_value = REACHED_200

        report = self.run(qt_dag, state_manager, **{missed: {"reached": False, "status": None}})

        assert report["verdict"] == "ko"

    def test_access_not_restored_is_a_failure(self, qt_dag, s3, state_manager):
        s3.quarantine_service.probe_bucket_until.return_value = {"reached": False, "status": 403, "attempts": 40, "seconds": 1170}

        with pytest.raises(RuntimeError, match="still answers HTTP 403 after the quarantine was lifted"):
            self.run(qt_dag, state_manager)

        state_manager.push_state.assert_called_once()  # le compte rendu est quand même écrit
