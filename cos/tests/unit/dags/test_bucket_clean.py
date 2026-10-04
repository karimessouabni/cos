"""Tests des étapes du DAG ``cos.bucket.v1.clean`` reprises du fichier d'entreprise
(validate_bucket, get_cos_api_key, is_bucket_empty). Les étapes suivantes seront
couvertes une fois reportées."""
import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.status import Status


def bucket_row(**overrides) -> dict:
    row = {
        "subscription_id": "sub-1",
        "name": "bucket-a",
        "clean_status": Status.SUCCESS.value,
        "virtual_server_endpoint": "https://vpe/bucket-a",
        "cos": {"crn": "crn:cos"},
    }
    row.update(overrides)
    return row


@pytest.fixture
def clean_dag(load_dag):
    return load_dag("cos.bucket.v1.clean.py")


@pytest.fixture
def payload(clean_dag):
    return clean_dag.module.BucketCleanPayload(subscription_id="sub-1")


def test_dag_identity(clean_dag):
    assert clean_dag.module.bucket_clean.dag_name == "cos.bucket.v1.clean"
    assert clean_dag.module.bucket_clean.config.options == {"lock_subscription_on_failure": False}
    assert list(clean_dag.steps)[:3] == ["validate_bucket", "get_cos_api_key", "is_bucket_empty"]


class TestValidateBucket:
    def run(self, clean_dag, payload, state_manager):
        return clean_dag.steps["validate_bucket"](session="session", state_manager=state_manager, payload=payload)

    def test_marks_the_clean_in_progress_and_returns_the_bucket(self, clean_dag, services, payload, state_manager):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row()

        assert self.run(clean_dag, payload, state_manager) == bucket_row()

        services.bucketService.get_bucket_by_sub_id.assert_called_once_with("session", "sub-1")
        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.INPROGRESS.value})

    @pytest.mark.parametrize("row", [None, bucket_row(name=None), bucket_row(name="")])
    def test_missing_or_unfinished_bucket_is_declined(self, clean_dag, services, payload, state_manager, row):
        services.bucketService.get_bucket_by_sub_id.return_value = row

        with pytest.raises(DeclineDemandException, match="doesn't exist or not fully created"):
            self.run(clean_dag, payload, state_manager)

        services.bucketService.update_bucket_clean_status.assert_not_called()
        state_manager.push_state.assert_not_called()

    def test_clean_already_in_progress_is_declined(self, clean_dag, services, payload, state_manager):
        services.bucketService.get_bucket_by_sub_id.return_value = bucket_row(clean_status=Status.INPROGRESS.value)

        with pytest.raises(DeclineDemandException, match="already in progress"):
            self.run(clean_dag, payload, state_manager)

        services.bucketService.update_bucket_clean_status.assert_not_called()


class TestGetCosApiKey:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["get_cos_api_key"](
            bucket=bucket_row(), session="session", state_manager=state_manager, reader="reader", vault="vault"
        )

    def test_reads_the_key_through_vault(self, clean_dag, services, state_manager):
        services.vault_service.get_cos_api_key.return_value = "api-key"

        assert self.run(clean_dag, state_manager) == "api-key"

        services.vault_service.get_cos_api_key.assert_called_once_with(bucket_row(), "vault", "reader")
        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        state_manager.push_state.assert_not_called()

    def test_vault_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, state_manager):
        services.vault_service.get_cos_api_key.side_effect = RuntimeError("vault down")

        with pytest.raises(RuntimeError, match="vault down"):
            self.run(clean_dag, state_manager)

        services.bucketService.update_bucket_clean_status.assert_called_with("sub-1", Status.FAILED, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.FAILED.value})


class TestIsBucketEmpty:
    def run(self, clean_dag, state_manager):
        return clean_dag.steps["is_bucket_empty"](
            bucket=bucket_row(), api_key="api-key", state_manager=state_manager, session="session"
        )

    @pytest.fixture
    def s3(self, services):
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"
        return services

    @pytest.mark.parametrize("has_contents, expected", [(False, True), (True, False)])
    def test_answers_from_the_bucket_listing(self, clean_dag, s3, state_manager, has_contents, expected):
        s3.bucketService.check_bucket_has_contents.return_value = has_contents

        assert self.run(clean_dag, state_manager) is expected

        s3.ibm_iam_service.get_iam_access_token.assert_called_once_with("api-key")
        s3.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())
        s3.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        state_manager.push_state.assert_not_called()

    def test_listing_failure_marks_the_clean_failed_and_reraises(self, clean_dag, s3, state_manager):
        s3.bucketService.check_bucket_has_contents.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager)

        s3.bucketService.update_bucket_clean_status.assert_called_with("sub-1", Status.FAILED, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.FAILED.value})
