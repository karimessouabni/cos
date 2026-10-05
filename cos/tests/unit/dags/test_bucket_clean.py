"""Tests des étapes du DAG ``cos.bucket.v1.clean`` (fichier d'entreprise)."""
import pytest

from airflow.sensors.base import PokeReturnValue

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
    assert list(clean_dag.steps) == [
        "validate_bucket",
        "get_cos_api_key",
        "is_bucket_empty",
        "create_expiration_rule",
        "save_create_expiration_rule_in_db",
        "scheduler_clean_bucket",
        "delete_expiration_rule",
        "save_delete_expiration_rule_in_db",
    ]


def test_only_the_scheduler_is_a_sensor(clean_dag):
    # Reschedule toutes les 3 h : le worker n'est pas occupé entre deux sondages.
    assert clean_dag.steps["scheduler_clean_bucket"].sensor_options == {
        "exponential_backoff": False, "poke_interval": 10800, "mode": "reschedule",
    }
    assert all(clean_dag.steps[name].sensor_options is None for name in clean_dag.steps if name != "scheduler_clean_bucket")


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


@pytest.fixture
def payload_with_requestor(clean_dag):
    return clean_dag.module.BucketCleanPayload(subscription_id="sub-1", requestor="karim")


class TestCreateExpirationRule:
    def run(self, clean_dag, state_manager, is_bucket_empty):
        return clean_dag.steps["create_expiration_rule"](
            api_key="api-key", bucket=bucket_row(), is_bucket_empty=is_bucket_empty,
            state_manager=state_manager, session="session",
        )

    def test_non_empty_bucket_gets_the_rule(self, clean_dag, services, state_manager):
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"
        services.bucketService.create_expiration_rule.return_value = True

        assert self.run(clean_dag, state_manager, is_bucket_empty=False) is True

        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")
        services.bucketService.create_expiration_rule.assert_called_once_with("tok", bucket_row())
        state_manager.push_state.assert_not_called()

    def test_empty_bucket_needs_no_rule(self, clean_dag, services, state_manager):
        assert self.run(clean_dag, state_manager, is_bucket_empty=True) is False

        services.bucketService.create_expiration_rule.assert_not_called()
        services.ibm_iam_service.get_iam_access_token.assert_not_called()

    def test_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, state_manager):
        services.bucketService.create_expiration_rule.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager, is_bucket_empty=False)

        services.bucketService.update_bucket_clean_status.assert_called_with("sub-1", Status.FAILED, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.FAILED.value})


class TestSaveCreateExpirationRuleInDb:
    def run(self, clean_dag, payload_with_requestor, created):
        return clean_dag.steps["save_create_expiration_rule_in_db"](
            is_expiration_created=created, bucket=bucket_row(), payload=payload_with_requestor, session="session"
        )

    def test_rule_created_is_recorded_after_disabling_the_previous_ones(self, clean_dag, services, payload_with_requestor):
        assert self.run(clean_dag, payload_with_requestor, created=True) is None

        lifecycle = services.lifecyclePolicyRuleService
        lifecycle.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_called_once_with(bucket_row(), "karim", "session")
        # Règle "clean_bucket" sur tout le bucket (préfixe vide) : objets courants et
        # versions non courantes expirent après 1 jour.
        lifecycle.complete_lifecycle_policy_rule_creation.assert_called_once_with(
            bucket_row(), "clean_bucket", "", 1, 1, "karim", "session"
        )

    def test_nothing_recorded_without_a_rule(self, clean_dag, services, payload_with_requestor):
        self.run(clean_dag, payload_with_requestor, created=False)

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_not_called()
        services.lifecyclePolicyRuleService.complete_lifecycle_policy_rule_creation.assert_not_called()

    def test_db_failure_reraises(self, clean_dag, services, payload_with_requestor):
        services.lifecyclePolicyRuleService.complete_lifecycle_policy_rule_creation.side_effect = RuntimeError("db")

        with pytest.raises(RuntimeError, match="db"):
            self.run(clean_dag, payload_with_requestor, created=True)


class TestSchedulerCleanBucket:
    def run(self, clean_dag, state_manager, created):
        return clean_dag.steps["scheduler_clean_bucket"](
            bucket=bucket_row(), api_key="api-key", is_expiration_created=created,
            state_manager=state_manager, session="session",
        )

    @pytest.mark.parametrize("has_contents, is_done", [(True, False), (False, True)])
    def test_pokes_until_the_bucket_is_empty(self, clean_dag, services, state_manager, has_contents, is_done):
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"
        services.bucketService.check_bucket_has_contents.return_value = has_contents

        result = self.run(clean_dag, state_manager, created=True)

        assert isinstance(result, PokeReturnValue)
        assert (result.is_done, result.xcom_value) == (is_done, {"content": "clean"})
        services.bucketService.check_bucket_has_contents.assert_called_once_with("tok", bucket_row())
        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.INPROGRESS, "session")

    def test_without_a_rule_the_sensor_is_done_at_once(self, clean_dag, services, state_manager):
        result = self.run(clean_dag, state_manager, created=False)

        assert (result.is_done, result.xcom_value) == (True, {"content": "clean"})
        services.bucketService.check_bucket_has_contents.assert_not_called()
        services.bucketService.update_bucket_clean_status.assert_not_called()

    def test_listing_failure_marks_the_clean_failed_and_reraises(self, clean_dag, services, state_manager):
        services.bucketService.check_bucket_has_contents.side_effect = RuntimeError("s3 down")

        with pytest.raises(RuntimeError, match="s3 down"):
            self.run(clean_dag, state_manager, created=True)

        services.bucketService.update_bucket_clean_status.assert_called_with("sub-1", Status.FAILED, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.FAILED.value})


class S3Error(Exception):
    def __init__(self, code):
        super().__init__(f"s3 {code}")
        self.code = code


class TestDeleteExpirationRule:
    def run(self, clean_dag, state_manager, check_clean_done):
        return clean_dag.steps["delete_expiration_rule"](
            bucket=bucket_row(), api_key="api-key", check_clean_done=check_clean_done,
            state_manager=state_manager, session="session",
        )

    def test_rule_removed_once_the_bucket_is_clean(self, clean_dag, services, state_manager):
        services.ibm_iam_service.get_iam_access_token.return_value = "tok"

        assert self.run(clean_dag, state_manager, check_clean_done=PokeReturnValue(is_done=True)) is True

        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.SUCCESS, "session")
        state_manager.push_state.assert_called_once_with({"clean_status": Status.SUCCESS.value})
        services.bucketService.delete_lifecycle_policy.assert_called_once_with("tok", bucket_row())

    def test_nothing_to_remove_without_a_sensor_result(self, clean_dag, services, state_manager):
        assert self.run(clean_dag, state_manager, check_clean_done=None) is False

        services.bucketService.delete_lifecycle_policy.assert_not_called()
        services.bucketService.update_bucket_clean_status.assert_not_called()

    def test_missing_policy_is_tolerated(self, clean_dag, services, state_manager):
        """404 au delete : la règle a déjà disparu, l'étape n'échoue pas (et ne renvoie rien)."""
        services.bucketService.delete_lifecycle_policy.side_effect = S3Error(404)

        assert self.run(clean_dag, state_manager, check_clean_done=PokeReturnValue(is_done=True)) is None

        # Le SUCCESS a été posé avant l'appel, il reste.
        services.bucketService.update_bucket_clean_status.assert_called_once_with("sub-1", Status.SUCCESS, "session")

    def test_other_s3_error_marks_the_clean_failed_and_reraises(self, clean_dag, services, state_manager):
        services.bucketService.delete_lifecycle_policy.side_effect = S3Error(500)

        with pytest.raises(S3Error):
            self.run(clean_dag, state_manager, check_clean_done=PokeReturnValue(is_done=True))

        services.bucketService.update_bucket_clean_status.assert_called_with("sub-1", Status.FAILED, "session")
        state_manager.push_state.assert_called_with({"clean_status": Status.FAILED.value})

    def test_error_without_code_is_masked_by_an_attribute_error(self, clean_dag, services, state_manager):
        """Point ouvert : ``str(e.code)`` suppose une erreur S3. Une autre exception
        (IAM, réseau) lève AttributeError à la place, et le statut FAILED n'est pas posé."""
        services.ibm_iam_service.get_iam_access_token.side_effect = RuntimeError("iam down")

        with pytest.raises(AttributeError):
            self.run(clean_dag, state_manager, check_clean_done=PokeReturnValue(is_done=True))

        services.bucketService.update_bucket_clean_status.assert_not_called()


class TestSaveDeleteExpirationRuleInDb:
    def run(self, clean_dag, payload_with_requestor, deleted):
        return clean_dag.steps["save_delete_expiration_rule_in_db"](
            is_expiration_deleted=deleted, bucket=bucket_row(), payload=payload_with_requestor, session="session"
        )

    def test_rules_disabled_once_the_policy_is_gone(self, clean_dag, services, payload_with_requestor):
        self.run(clean_dag, payload_with_requestor, deleted=True)

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_called_once_with(
            bucket_row(), "karim", "session"
        )

    @pytest.mark.parametrize("deleted", [False, None])
    def test_nothing_disabled_otherwise(self, clean_dag, services, payload_with_requestor, deleted):
        self.run(clean_dag, payload_with_requestor, deleted=deleted)

        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.assert_not_called()

    def test_db_failure_reraises(self, clean_dag, services, payload_with_requestor):
        services.lifecyclePolicyRuleService.disable_lifecycle_policy_rules_by_bucket_sub_id.side_effect = RuntimeError("db")

        with pytest.raises(RuntimeError, match="db"):
            self.run(clean_dag, payload_with_requestor, deleted=True)
