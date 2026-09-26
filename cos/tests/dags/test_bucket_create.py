"""Tests des étapes du DAG ``cos.bucket.v1.create``.

Chaque étape est appelée comme une fonction (voir la fixture ``dag`` dans
``conftest.py``) avec ses dépendances passées explicitement. Les modules
``cos_service.services.*`` sont des ``MagicMock`` (fixture ``services``), sauf
``immutability_service`` qui est le vrai.
"""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from bp2i_terraform.backends.schematics import TerraformVar
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.bucket_retention import BucketRetention
from cos_service.schemas.immutability import Immutability
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus


# --- helpers ------------------------------------------------------------------

class FakeRow(dict):
    """Ligne ORM factice : supporte ``dict(row)`` et ``row.attribut``."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


REALM = {
    "name": "realm-a",
    "realm_apcode_details": [{"apcode": "AP1"}],
    "wklapp_account_number": "wk-123",
}

COS_INSTANCE = FakeRow(
    subscription_id="cos-sub",
    crn="crn:cos",
    name="cos-a",
    environment="dev",
    context={"realm": "realm-a", "app_code": "AP1", "wklapp_account_name": "wklapp-a"},
)

BACKUP_VAULT = {"subscription_id": "bv-sub", "crn": "crn:bv"}

ACCOUNT_CRNS = {"cloudlogs": "crn:logs", "encryption_key": "crn:kms"}

SECRETS = {
    "vault_read_token": "rt",
    "vault_read_addr": "ra",
    "vault_write_token": "wt",
    "vault_write_addr": "wa",
    "gitlab_token": "gl",
}

TF_OUTPUTS = {"bucket_name": {"value": "bucket-a"}, "bucket_crn": {"value": "crn:bucket"}}


def fresh_immutability(versioning=False, choice=Immutability.NONE) -> dict:
    return {
        "immutability_choice": choice.value,
        "object_locking_enabled": False,
        "object_versioning_enabled": versioning,
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "retention": {"retention_enabled": False, "default": None, "minimum": None, "maximum": None},
        "backup": {"backup_enabled": False, "backup_vault_sub_id": None, "backup_retention_days": None},
    }


def validated(backup_vault=None) -> dict:
    return {"realm": REALM, "cos_instance": dict(COS_INSTANCE), "backup_vault": backup_vault}


@pytest.fixture
def make_payload(dag):
    def factory(**overrides):
        fields = dict(
            realm="realm-a",
            apcode="AP1",
            environment="dev",
            region="eu-de",
            subscription_id="sub-1",
            cos_instance="co21000001",
            storage_class=dag.module.BucketCreatePayload.StorageClass.STANDARD,
        )
        fields.update(overrides)
        return dag.module.BucketCreatePayload(**fields)

    return factory


@pytest.fixture
def state_manager():
    manager = MagicMock(name="state_manager")
    manager.get_subscription.return_value.description = "my bucket"
    return manager


@pytest.fixture
def happy_services(services):
    """Mocks configurés pour une demande valide."""
    services.contextService.get_realm.return_value = REALM
    services.contextService.get_apcodes.return_value = ["AP1"]
    services.contextService.get_account_instances_crn.return_value = ACCOUNT_CRNS
    services.cosService.get_cos_instance_by_name.return_value = COS_INSTANCE
    services.cosService.get_cos_instance_status.return_value = SubscriptionStatus.ACTIVE.value
    services.backup_vault_service.get_backup_vault_by_name.return_value = BACKUP_VAULT
    services.vault_service.get_vault_secrets.return_value = SECRETS
    return services


# --- câblage ------------------------------------------------------------------

def test_dag_declares_the_expected_steps_in_order(dag):
    assert list(dag.steps) == [
        "validate_request",
        "process_protection_configuration",
        "get_account_instances_crn",
        "create_tf_workspace",
        "apply_tf_workspace",
        "save_bucket_in_db",
    ]


def test_payload_defaults(make_payload):
    payload = make_payload()

    assert payload.immutability_choice is Immutability.NONE
    assert payload.enable_custom_permissions is False
    assert payload.retention is None
    assert payload.backup is None


# --- validate_request -----------------------------------------------------------

class TestValidateRequest:
    def run(self, dag, payload, state_manager):
        return dag.steps["validate_request"](payload=payload, session="session", state_manager=state_manager)

    def errors_of(self, dag, payload, state_manager) -> list[str]:
        with pytest.raises(DeclineDemandException) as excinfo:
            self.run(dag, payload, state_manager)
        return str(excinfo.value).split(" | ")

    def test_valid_request_returns_realm_instance_and_no_vault(self, dag, happy_services, make_payload, state_manager):
        result = self.run(dag, make_payload(), state_manager)

        assert result == {"realm": REALM, "cos_instance": dict(COS_INSTANCE), "backup_vault": None}
        state_manager.push_state.assert_called_once_with({"cos_instance": "co21000001"})
        happy_services.backup_vault_service.get_backup_vault_by_name.assert_not_called()

    def test_empty_realm_does_not_call_the_context_service(self, dag, happy_services, make_payload, state_manager):
        errors = self.errors_of(dag, make_payload(realm=""), state_manager)

        # Le contexte de l'instance COS ne correspond pas non plus à un realm vide :
        # les deux erreurs remontent ensemble.
        assert errors[0] == "the realm is empty"
        assert any("context of bucket is different" in error for error in errors)
        happy_services.contextService.get_realm.assert_not_called()

    def test_unknown_realm(self, dag, happy_services, make_payload, state_manager):
        happy_services.contextService.get_realm.return_value = None

        assert self.errors_of(dag, make_payload(), state_manager) == ["the realm realm-a doesn't exist"]

    def test_realm_404_is_treated_as_unknown(self, dag, happy_services, make_payload, state_manager):
        happy_services.contextService.get_realm.return_value = {"status": 404}

        errors = self.errors_of(dag, make_payload(), state_manager)

        assert errors == ["the realm realm-a doesn't exist"]
        happy_services.contextService.get_apcodes.assert_not_called()

    def test_realm_without_apcodes(self, dag, happy_services, make_payload, state_manager):
        happy_services.contextService.get_realm.return_value = {**REALM, "realm_apcode_details": None}

        assert self.errors_of(dag, make_payload(), state_manager) == ["there is no apcodes on this realm realm-a"]

    def test_apcode_not_in_realm(self, dag, happy_services, make_payload, state_manager):
        happy_services.contextService.get_apcodes.return_value = ["OTHER"]

        assert self.errors_of(dag, make_payload(), state_manager) == [
            "appCode AP1 doesn't belong to this realm realm-a"
        ]

    def test_missing_cos_instance_does_not_crash_and_keeps_other_errors(
        self, dag, happy_services, make_payload, state_manager
    ):
        happy_services.contextService.get_realm.return_value = None
        happy_services.cosService.get_cos_instance_by_name.return_value = None
        happy_services.backup_vault_service.get_backup_vault_by_name.return_value = None
        payload = make_payload(backup=BucketBackup(backup_enabled=True, backup_vault_name="nope"))

        errors = self.errors_of(dag, payload, state_manager)

        assert errors == [
            "the realm realm-a doesn't exist",
            "the cos instance co21000001 doesn't exist",
            "The Backup Vault doesn't exist for the name : nope",
        ]
        happy_services.cosService.get_cos_instance_status.assert_not_called()

    def test_inactive_cos_instance(self, dag, happy_services, make_payload, state_manager):
        happy_services.cosService.get_cos_instance_status.return_value = "locked"

        assert self.errors_of(dag, make_payload(), state_manager) == ["Bad cos instance status : locked"]

    def test_context_mismatch(self, dag, happy_services, make_payload, state_manager):
        happy_services.contextService.get_apcodes.return_value = ["AP2"]

        errors = self.errors_of(dag, make_payload(apcode="AP2"), state_manager)

        assert errors == [
            "The context of bucket is different from context of cos, realm cos realm-a , apcode cos AP1"
        ]

    def test_environment_mismatch(self, dag, happy_services, make_payload, state_manager):
        errors = self.errors_of(dag, make_payload(environment="prod"), state_manager)

        assert errors == ["Bucket environment : prod is different from cos environment : dev"]

    def test_backup_without_vault_name(self, dag, happy_services, make_payload, state_manager):
        payload = make_payload(backup=BucketBackup(backup_enabled=True))

        assert self.errors_of(dag, payload, state_manager) == [
            "The Backup Vault name is required to enable bucket backup"
        ]

    def test_backup_vault_is_resolved_and_returned(self, dag, happy_services, make_payload, state_manager):
        payload = make_payload(backup=BucketBackup(backup_enabled=True, backup_vault_name="vault-a"))

        result = self.run(dag, payload, state_manager)

        happy_services.backup_vault_service.get_backup_vault_by_name.assert_called_once_with("vault-a", "session")
        assert result["backup_vault"] == BACKUP_VAULT

    def test_disabled_backup_does_not_look_up_the_vault(self, dag, happy_services, make_payload, state_manager):
        payload = make_payload(backup=BucketBackup(backup_enabled=False, backup_vault_name="vault-a"))

        result = self.run(dag, payload, state_manager)

        assert result["backup_vault"] is None
        happy_services.backup_vault_service.get_backup_vault_by_name.assert_not_called()


# --- process_protection_configuration ----------------------------------------------

class TestProcessProtectionConfiguration:
    def run(self, dag, payload, backup_vault=None):
        return dag.steps["process_protection_configuration"](validated=validated(backup_vault), payload=payload)

    def test_no_option_gives_a_plain_bucket(self, dag, make_payload):
        result = self.run(dag, make_payload())

        assert result["immutability_choice"] == Immutability.NONE.value
        assert result["object_versioning_enabled"] is False
        assert result["backup"]["backup_enabled"] is False

    def test_versioning_none_means_false(self, dag, make_payload):
        assert self.run(dag, make_payload(enable_versioning=None))["object_versioning_enabled"] is False

    def test_retention_is_stored_in_days(self, dag, make_payload):
        payload = make_payload(retention=BucketRetention(
            retention_enabled=True, default_years=1, minimum_years=1, maximum_years=2
        ))

        result = self.run(dag, payload)

        assert result["immutability_choice"] == Immutability.RETENTION.value
        assert result["retention"]["default"] == 365

    def test_legacy_retention_format_is_still_accepted(self, dag, make_payload):
        payload = make_payload(retention=BucketRetention(retention_enabled=True, default=30, minimum=10, maximum=60))

        result = self.run(dag, payload)

        assert result["immutability_choice"] == Immutability.RETENTION.value
        assert result["retention"] == {"retention_enabled": True, "default": 30, "minimum": 10, "maximum": 60}

    def test_object_lock_needs_versioning(self, dag, make_payload):
        with pytest.raises(DeclineDemandException, match="Versioning should be enabled"):
            self.run(dag, make_payload(object_lock_duration_days=30))

    def test_enabled_backup_uses_the_resolved_vault(self, dag, make_payload):
        backup = BucketBackup(backup_enabled=True, backup_vault_name="vault-a", backup_retention_days=7)
        payload = make_payload(enable_versioning=True, backup=backup)

        result = self.run(dag, payload, backup_vault=BACKUP_VAULT)

        assert result["backup"] == {
            "backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7
        }

    def test_explicitly_disabled_backup_is_not_declined(self, dag, make_payload):
        payload = make_payload(enable_versioning=False, backup=BucketBackup(backup_enabled=False))

        result = self.run(dag, payload)

        assert result["backup"]["backup_enabled"] is False

    def test_explicitly_disabled_backup_does_not_conflict_with_retention(self, dag, make_payload):
        payload = make_payload(
            retention=BucketRetention(retention_enabled=True, default_days=30, minimum_days=10, maximum_days=60),
            backup=BucketBackup(backup_enabled=False),
        )

        result = self.run(dag, payload)

        assert result["retention"]["retention_enabled"] is True


# --- get_account_instances_crn -----------------------------------------------------

def test_get_account_instances_crn_uses_the_wklapp_account_name(dag, happy_services):
    result = dag.steps["get_account_instances_crn"](validated=validated())

    happy_services.contextService.get_account_instances_crn.assert_called_once_with("wklapp-a")
    assert result == ACCOUNT_CRNS


# --- create_tf_workspace -------------------------------------------------------------

class TestCreateTfWorkspace:
    def run(self, dag, payload, state_manager, immutability=None, backup_vault=None):
        return dag.steps["create_tf_workspace"](
            validated=validated(backup_vault),
            account_instances_crn=ACCOUNT_CRNS,
            immutability=immutability or fresh_immutability(),
            tf="tf",
            state_manager=state_manager,
            session="session",
            payload=payload,
            vault="vault",
        )

    def test_creates_bucket_row_then_workspace(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = None
        happy_services.bucketService.process_bucket_creation.return_value = {"workspace": {"workspace_id": None}}
        happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}

        result = self.run(dag, make_payload(), state_manager, backup_vault=BACKUP_VAULT)

        assert result == "ws-1"
        happy_services.bucketService.process_bucket_creation.assert_called_once()
        happy_services.bucketService.update_bucket_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.CREATING, "session"
        )
        state_manager.push_state.assert_called_once_with({"workspace_name": "ws_bucket_sub-1"})

    def test_bucket_row_gets_the_orm_cos_and_vault_rows(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = None
        happy_services.bucketService.process_bucket_creation.return_value = {"workspace": {"workspace_id": "ws-1"}}
        vault_row = object()
        happy_services.backup_vault_service.get_backup_vault_by_sub_id.return_value = vault_row
        immutability = fresh_immutability(versioning=True)
        immutability["backup"] = {"backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7}

        self.run(dag, make_payload(), state_manager, immutability=immutability, backup_vault=BACKUP_VAULT)

        happy_services.backup_vault_service.get_backup_vault_by_sub_id.assert_called_once_with("bv-sub", "session")
        args = happy_services.bucketService.process_bucket_creation.call_args.args
        assert args[5] is COS_INSTANCE  # ligne ORM, pas le dict de `validated`
        assert args[6] is vault_row

    def test_terraform_variables_come_from_validated_data(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": None}}
        happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}

        self.run(dag, make_payload(region="eu-de"), state_manager, backup_vault=BACKUP_VAULT)

        call = happy_services.schematics_service.create_or_update_ws.call_args
        assert call.args == ("tf",)
        assert call.kwargs["workspace_name"] == "ws_bucket_sub-1"
        assert call.kwargs["tf_directory"] == "terraform/v1.12/bucket"
        assert call.kwargs["description"] == "my bucket"
        assert call.kwargs["gitlab_token"] == "gl"
        variables = call.kwargs["variables"]
        assert variables["cos_instance_crn"] == "crn:cos"
        assert variables["cos_instance_name"] == "cos-a"
        assert variables["wklapp_account_id"] == "wk-123"
        assert variables["target_backup_vault_crn"] == "crn:bv"
        assert variables["kms_key_crn"] == "crn:kms"
        assert variables["cloud_type"] == "3"
        assert isinstance(variables["vault_read_token"], TerraformVar)
        happy_services.vault_service.get_vault_secrets.assert_called_once_with(
            realm="realm-a", apcode="AP1", vault="vault"
        )

    def test_no_realm_or_vault_query_is_repeated(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": "ws-1"}}

        self.run(dag, make_payload(), state_manager, backup_vault=BACKUP_VAULT)

        happy_services.contextService.get_realm.assert_not_called()
        happy_services.backup_vault_service.get_backup_vault_by_sub_id.assert_not_called()
        happy_services.cosService.get_cos_instance_by_name.assert_not_called()

    def test_cloud_type_outside_eu_de(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": None}}
        happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}

        self.run(dag, make_payload(region="eu-fr2"), state_manager)

        variables = happy_services.schematics_service.create_or_update_ws.call_args.kwargs["variables"]
        assert variables["cloud_type"] == "2"
        assert variables["target_backup_vault_crn"] is None

    def test_bucket_dict_without_workspace_key_still_creates_the_workspace(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"subscription_id": "sub-1"}
        happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}

        assert self.run(dag, make_payload(), state_manager) == "ws-1"

    def test_existing_workspace_is_reused(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": "ws-old"}}

        result = self.run(dag, make_payload(), state_manager)

        assert result == "ws-old"
        happy_services.schematics_service.create_or_update_ws.assert_not_called()
        happy_services.bucketService.process_bucket_creation.assert_not_called()

    def test_failed_bucket_insert_locks_the_bucket_and_reraises(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = None
        happy_services.bucketService.process_bucket_creation.side_effect = RuntimeError("db down")

        with pytest.raises(RuntimeError, match="db down"):
            self.run(dag, make_payload(), state_manager)

        happy_services.bucketService.update_bucket_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.LOCKED, "session"
        )
        happy_services.bucketService.update_bucket_workspace_status.assert_called_once_with(
            "sub-1", Status.FAILED, "session"
        )

    def test_failed_workspace_creation_locks_the_bucket(self, dag, happy_services, make_payload, state_manager):
        happy_services.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": None}}
        happy_services.schematics_service.create_or_update_ws.side_effect = RuntimeError("schematics")

        with pytest.raises(RuntimeError):
            self.run(dag, make_payload(), state_manager)

        happy_services.bucketService.update_bucket_status.assert_called_once_with(
            "sub-1", SubscriptionStatus.LOCKED, "session"
        )


# --- apply_tf_workspace ---------------------------------------------------------------

class TestApplyTfWorkspace:
    def run(self, dag, payload, state_manager, backup_vault=None):
        return dag.steps["apply_tf_workspace"](
            validated=validated(backup_vault),
            workspace_id="ws-1",
            account_instances_crn=ACCOUNT_CRNS,
            immutability=fresh_immutability(),
            tf="tf",
            state_manager=state_manager,
            session="session",
            payload=payload,
            vault="vault",
        )

    def test_updates_the_workspace_then_runs_it(self, dag, happy_services, make_payload, state_manager):
        happy_services.schematics_service.run_workspace.return_value = TF_OUTPUTS

        result = self.run(dag, make_payload(), state_manager, backup_vault=BACKUP_VAULT)

        assert result == TF_OUTPUTS
        happy_services.schematics_service.run_workspace.assert_called_once_with("tf", "ws-1")
        happy_services.workspaceService.update_bucket_workspace.assert_called_once()
        happy_services.bucketService.update_bucket_workspace_status.assert_called_once_with(
            "sub-1", Status.INPROGRESS, "session"
        )
        details = happy_services.workspaceService.build_bucket_workspace_details.call_args.kwargs
        assert details["cos_instance_crn"] == "crn:cos"
        assert details["backup_vault_crn"] == "crn:bv"
        assert details["description"] == "my bucket"
        happy_services.cosService.get_cos_instance_by_name.assert_not_called()

    def test_failed_apply_locks_the_bucket_and_reraises(self, dag, happy_services, make_payload, state_manager):
        happy_services.schematics_service.run_workspace.side_effect = RuntimeError("apply failed")

        with pytest.raises(RuntimeError, match="apply failed"):
            self.run(dag, make_payload(), state_manager)

        happy_services.bucketService.update_bucket_status.assert_called_with(
            "sub-1", SubscriptionStatus.LOCKED, "session"
        )
        happy_services.bucketService.update_bucket_workspace_status.assert_called_with(
            "sub-1", Status.FAILED, "session"
        )


# --- save_bucket_in_db -------------------------------------------------------------

class TestSaveBucketInDb:
    def run(self, dag, payload, state_manager, immutability=None):
        return dag.steps["save_bucket_in_db"](
            apply_tf_result=TF_OUTPUTS,
            immutability=immutability or fresh_immutability(),
            payload=payload,
            state_manager=state_manager,
            session="session",
        )

    def test_pushes_the_effective_configuration_once(self, dag, happy_services, make_payload, state_manager):
        immutability = fresh_immutability(versioning=True, choice=Immutability.OBJECT_LOCK)
        immutability["object_locking_enabled"] = True
        immutability["object_lock_duration_days"] = 30

        result = self.run(dag, make_payload(region="eu-de"), state_manager, immutability)

        assert result == {"subscription_id": "sub-1"}
        state_manager.push_state.assert_called_once()
        state = state_manager.push_state.call_args.args[0]
        assert state["name"] == "bucket-a"
        assert state["crn"] == "crn:bucket"
        assert state["immutability_choice"] == "object_lock"
        assert state["object_locking_enabled"] is True
        assert state["object_lock_duration_days"] == 30
        assert state["enable_versioning"] is True
        assert state["retention"] == immutability["retention"]
        assert state["backup"] == immutability["backup"]
        assert state["clean_status"] == Status.SUCCESS.value
        assert state["lifecycle_policy_rule_enabled"] is False

    def test_retention_state_keeps_days_keys_and_echoes_years_as_sent(self, dag, happy_services, make_payload, state_manager):
        immutability = fresh_immutability(choice=Immutability.RETENTION_YEARLY)
        immutability["retention"] = {"retention_enabled": True, "default": 365, "minimum": 365, "maximum": 730}
        payload = make_payload(retention=BucketRetention(
            retention_enabled=True, default_years=1, minimum_years=1, maximum_years=2
        ))

        self.run(dag, payload, state_manager, immutability)

        assert state_manager.push_state.call_args.args[0]["retention"] == {
            "retention_enabled": True, "default": 365, "minimum": 365, "maximum": 730,
            "unit": "years", "default_years": 1, "minimum_years": 1, "maximum_years": 2,
        }

    def test_defaults_are_pushed_even_when_the_client_sent_nothing(self, dag, happy_services, make_payload, state_manager):
        self.run(dag, make_payload(), state_manager)

        state = state_manager.push_state.call_args.args[0]
        assert state["immutability_choice"] == "none"
        assert state["retention"]["retention_enabled"] is False
        assert state["backup"]["backup_enabled"] is False

    def test_eu_de_only_has_host_style_endpoint(self, dag, happy_services, make_payload, state_manager):
        self.run(dag, make_payload(region="eu-de"), state_manager)

        endpoint = state_manager.push_state.call_args.args[0]["virtual_server_endpoint"]
        assert endpoint == {"host_style": "https://bucket-a.s3.direct.eu-de.cloud-object-storage.appdomain.cloud"}

    def test_eu_fr2_also_has_path_style_endpoint(self, dag, happy_services, make_payload, state_manager):
        self.run(dag, make_payload(region="eu-fr2"), state_manager)

        endpoint = state_manager.push_state.call_args.args[0]["virtual_server_endpoint"]
        assert endpoint == {
            "path_style": "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud/bucket-a",
            "host_style": "https://bucket-a.s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud",
        }

    def test_completes_the_bucket_with_its_vip(self, dag, happy_services, make_payload, state_manager):
        self.run(dag, make_payload(region="eu-de"), state_manager)

        happy_services.bucketService.complete_bucket_create.assert_called_once_with(
            "sub-1",
            "https://s3.direct.eu-de.cloud-object-storage.appdomain.cloud/bucket-a",
            TF_OUTPUTS,
            "session",
        )
