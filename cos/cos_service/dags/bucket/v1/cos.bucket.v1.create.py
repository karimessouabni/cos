from bp2i_airflow_library import add_project_to_path
from enum import Enum
from typing import Optional

add_project_to_path()

import logging
from pathlib import Path

from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_airflow_library.dag import product_action, step
from bp2i_airflow_library.dependencies import (
    SASession,
    SchematicsBackend,
    StateManager,
    Vault,
    airflow_context_dependency,
    depends,
    payload_dependency,
    smart_schematics_backend_dependency,
    sqlalchemy_session_dependency,
    state_manager_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import (
    DeclineDemandException,
    mark_task_as_declined,
)
from bp2i_airflow_library.schemas import Field, ProductCreatePayload

from cos_service.schemas.bucket_retention import BucketRetention
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.immutability import Immutability
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus

logger = logging.getLogger(__name__)

class BucketCreatePayload(ProductCreatePayload):
    class StorageClass(str, Enum):
        STANDARD = "standard"
        VAULT = "vault"
        COLD = "cold"
        SMART = "smart"

    storage_class: StorageClass
    cos_instance: str = Field(default="co21000001", minLength=0, maxLength=20)
    retention: BucketRetention | None = Field(updatable=True)
    object_lock_duration_days: Optional[int] | None = Field(updatable=True)
    object_lock_duration_years: Optional[int] | None = Field(updatable=True)
    enable_custom_permissions: Optional[bool] = Field(default=False, updatable=True)
    immutability_choice: Immutability = Field(default=Immutability.NONE, updatable=True)
    enable_versioning: Optional[bool] | None = Field(default=None, updatable=True)
    backup: BucketBackup | None = Field(updatable=True)


@product_action(Path(__file__).stem, tags=["cos"], payload=BucketCreatePayload)
def bucket_create():
    @step
    def validate_request(
        payload: BucketCreatePayload = depends(payload_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
    ) -> dict:
        """Checks everything the demand refers to and declines with all the errors at once.

        Returns {"realm": <dict>, "cos_instance": <dict>, "backup_vault": <dict | None>}
        so the following steps reuse what was fetched here instead of querying again.
        """
        from cos_service.services.contextService import get_realm, get_apcodes
        from cos_service.services.cosService import get_cos_instance_by_name, get_cos_instance_status
        from cos_service.services.backup_vault_service import get_backup_vault_by_name

        errors = []
        realm = None

        # --- realm / apcode ---------------------------------------------------
        if not payload.realm:
            errors.append("the realm is empty")
        else:
            realm = get_realm(payload.realm)
            if realm is None or str(realm.get("status", "")) == "404":
                errors.append(f"the realm {payload.realm} doesn't exist")
            elif not realm.get("realm_apcode_details"):
                errors.append(f"there is no apcodes on this realm {payload.realm}")
            elif payload.apcode not in get_apcodes(realm):
                errors.append(f"appCode {payload.apcode} doesn't belong to this realm {payload.realm}")

        # --- cos instance -----------------------------------------------------
        state_manager.push_state({"cos_instance": payload.cos_instance})
        cos_instance = get_cos_instance_by_name(payload.cos_instance, session)
        if cos_instance is None:
            errors.append(f"the cos instance {payload.cos_instance} doesn't exist")
        else:
            cos_instance_status = get_cos_instance_status(cos_instance.subscription_id)
            if cos_instance_status != SubscriptionStatus.ACTIVE.value:
                errors.append(f"Bad cos instance status : {cos_instance_status}")

            # --- bucket context must match cos instance context ---------------
            cos_instance = dict(cos_instance)
            context = cos_instance["context"]
            if context["realm"] != payload.realm or context["app_code"] != payload.apcode:
                errors.append(
                    f"The context of bucket is different from context of cos, "
                    f"realm cos {context['realm']} , apcode cos {context['app_code']}"
                )
            if cos_instance["environment"] != payload.environment:
                errors.append(
                    f"Bucket environment : {payload.environment} is different from "
                    f"cos environment : {cos_instance['environment']}"
                )

        # --- backup vault (only when a backup is requested) -------------------
        backup_vault = None
        if payload.backup is not None and payload.backup.backup_enabled is True:
            if not payload.backup.backup_vault_name:
                errors.append("The Backup Vault name is required to enable bucket backup")
            else:
                backup_vault = get_backup_vault_by_name(payload.backup.backup_vault_name, session)
                if backup_vault is None:
                    errors.append(
                        f"The Backup Vault doesn't exist for the name : {payload.backup.backup_vault_name}"
                    )

        if errors:
            raise DeclineDemandException(" | ".join(errors))

        return {"realm": realm, "cos_instance": cos_instance, "backup_vault": backup_vault}

    @step
    def process_protection_configuration(
        validated: dict,
        payload: BucketCreatePayload = depends(payload_dependency),
    ) -> dict:
        from cos_service.services.immutability_service import compute_bucket_new_immutability

        enable_versioning = payload.enable_versioning if payload.enable_versioning is not None else False

        # On a create, a backup that is absent or explicitly disabled is the same
        # thing: no backup. Only an enabled backup is handed to the service,
        # otherwise compute_bucket_backup would decline the demand on its
        # "disable backup" branch (which requires versioning) for a bucket that
        # never had a backup. The vault itself was resolved by validate_request.
        backup = payload.backup if payload.backup is not None and payload.backup.backup_enabled is True else None
        if backup is not None:
            backup.backup_vault_sub_id = validated["backup_vault"]["subscription_id"]
        logger.info("backup requested : %s", backup)

        immutability = {
            "object_locking_enabled": False,
            "object_versioning_enabled": enable_versioning,
            "object_lock_duration_days": None,
            "object_lock_duration_years": None,
            "retention": {"retention_enabled": False,
                          "default": None,
                          "minimum": None,
                          "maximum": None},
            "backup": {"backup_enabled": False, "backup_vault_sub_id": None, "backup_retention_days": None},
        }

        return compute_bucket_new_immutability(
            payload.immutability_choice,
            payload.retention,
            payload.object_lock_duration_days,
            payload.object_lock_duration_years,
            enable_versioning,
            immutability,
            backup,
        )

    @step
    def get_account_instances_crn(validated: dict) -> dict:
        from cos_service.services.contextService import get_account_instances_crn

        context = validated["cos_instance"]["context"]
        wklapp_account_name = context["wklapp_account_name"]
        account_instances_crn = get_account_instances_crn(wklapp_account_name)
        return account_instances_crn

    @step
    def create_tf_workspace(
        validated: dict,
        account_instances_crn: dict,
        immutability: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketCreatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> str:
        from bp2i_terraform.backends.schematics import TerraformVar
        from cos_service.services.schematics_service import TERRAFORM_VERSION, create_or_update_ws
        from cos_service.services.bucketService import (
            get_bucket_by_sub_id,
            process_bucket_creation,
            update_bucket_workspace_details,
            update_bucket_workspace_status,
            update_bucket_status,
        )
        from cos_service.services.cosService import get_cos_instance_by_name
        from cos_service.services.vault_service import get_vault_secrets
        from cos_service.services.backup_vault_service import get_backup_vault_by_sub_id

        realm = validated["realm"]
        cos_instance = validated["cos_instance"]
        backup_vault = validated["backup_vault"]

        secrets = get_vault_secrets(realm=realm.get("name", None), apcode=payload.apcode, vault=vault)
        ws_name = f"ws_bucket_{payload.subscription_id}"
        tf_directory = f"terraform/v{TERRAFORM_VERSION}/bucket"

        variables = {
            "region": payload.region,
            "bucket_storage_class": payload.storage_class,
            "cos_instance_crn": cos_instance["crn"],
            "cos_instance_name": cos_instance["name"],
            "activity_tracker_crn": account_instances_crn.get("cloudlogs", None),
            "kms_key_crn": account_instances_crn.get("encryption_key", None),
            "sysdig_crn": account_instances_crn.get("cloudlogs", None),
            "management_endpoint_type_for_bucket": "direct",
            "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
            "vault_read_addr": secrets["vault_read_addr"],
            "vault_write_token": TerraformVar(secrets["vault_write_token"], True),
            "app_code": payload.apcode,
            "vault_write_addr": secrets["vault_write_addr"],
            "orchestrator_environment": ENVIRONMENT,
            "wklapp_account_id": realm.get("wklapp_account_number", None),
            "retention": immutability["retention"],
            "enable_custom_permissions": payload.enable_custom_permissions,
            "object_locking_enabled": immutability["object_locking_enabled"],
            "object_versioning_enabled": immutability["object_versioning_enabled"],
            "object_lock_duration_days": immutability["object_lock_duration_days"],
            "object_lock_duration_years": immutability["object_lock_duration_years"],
            "backup_enabled": immutability["backup"]["backup_enabled"],
            "target_backup_vault_crn": backup_vault["crn"] if backup_vault is not None else None,
            "initial_delete_after_days": immutability["backup"]["backup_retention_days"],
            "cloud_type": "3" if payload.region == "eu-de" else "2",
        }

        description = state_manager.get_subscription().description

        try:
            bucket = get_bucket_by_sub_id(session, payload.subscription_id)
            if bucket is None:
                # process_bucket_creation assigns `bucket.cos` and
                # `bucket.backup_vault` (SQLAlchemy relationships): it needs the
                # ORM rows, not the serialised dicts carried in `validated`.
                cos_instance_row = get_cos_instance_by_name(payload.cos_instance, session)
                backup_vault_sub_id = immutability["backup"]["backup_vault_sub_id"]
                backup_vault_row = (
                    get_backup_vault_by_sub_id(backup_vault_sub_id, session)
                    if backup_vault_sub_id is not None
                    else None
                )
                bucket = process_bucket_creation(
                    payload, realm, immutability, account_instances_crn, description,
                    cos_instance_row, backup_vault_row, session,
                )

            if bucket["workspace"]["workspace_id"] is None:
                logger.info("Terraform create workspace")
                create_ws_result = create_or_update_ws(
                    tf,
                    workspace_name=ws_name,
                    orchestrator_env=ENVIRONMENT,
                    tf_directory=tf_directory,
                    variables=variables,
                    description=description,
                    gitlab_token=secrets["gitlab_token"],
                )

                update_bucket_status(payload.subscription_id, SubscriptionStatus.CREATING, session)
                update_bucket_workspace_details(bucket, create_ws_result, session)
                state_manager.push_state({"workspace_name": ws_name})
                return create_ws_result["id"]

            else:
                logger.info("Workspace already created")
                workspace = bucket['workspace']
                return workspace['workspace_id']

        except Exception:
            update_bucket_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_bucket_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def apply_tf_workspace(
        validated: dict,
        workspace_id: str,
        account_instances_crn: dict,
        immutability: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketCreatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict:
        from cos_service.services.workspaceService import (
            update_bucket_workspace,
            build_bucket_workspace_details,
        )
        from cos_service.services.bucketService import (
            update_bucket_workspace_status,
            update_bucket_status,
        )
        from cos_service.services.schematics_service import run_workspace

        cos_instance = validated["cos_instance"]
        backup_vault = validated["backup_vault"]

        workspace_details = build_bucket_workspace_details(
            workspace_id=workspace_id,
            realm=payload.realm,
            app_code=payload.apcode,
            region=payload.region,
            storage_class=payload.storage_class,
            cos_instance_crn=cos_instance["crn"],
            cos_instance_name=cos_instance["name"],
            activity_tracker_crn=account_instances_crn.get("cloudlogs", None),
            kms_crn=account_instances_crn.get("encryption_key", None),
            monitoring_crn=account_instances_crn.get("cloudlogs", None),
            management_endpoint_type="direct",
            immutability=immutability,
            enable_custom_permissions=payload.enable_custom_permissions,
            description=state_manager.get_subscription().description,
            backup_vault_crn=backup_vault["crn"] if backup_vault is not None else None
        )

        try:
            update_bucket_status(payload.subscription_id, SubscriptionStatus.CREATING, session)
            update_bucket_workspace_status(payload.subscription_id, Status.INPROGRESS, session)

            update_bucket_workspace(payload=payload, workspace_details=workspace_details, vault=vault, tf=tf)

            return run_workspace(tf, workspace_id)
        except Exception:
            update_bucket_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_bucket_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def save_bucket_in_db(
        apply_tf_result: dict,
        immutability: dict,
        payload: BucketCreatePayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict | None:
        from cos_service.services.bucketService import complete_bucket_create

        bucket_name = apply_tf_result["bucket_name"]["value"]
        vpe = f"s3.direct.{payload.region}.cloud-object-storage.appdomain.cloud"
        vip = f"https://{vpe}/{bucket_name}"

        virtual_server_endpoint = {
            "path_style": f"https://{vpe}/{bucket_name}",
            "host_style": f"https://{bucket_name}.{vpe}",
        } if payload.region == "eu-fr2" else {
            "host_style": f"https://{bucket_name}.{vpe}",
        }

        # The state reflects the effective configuration computed by the
        # immutability service (defaults included), not what the client typed.
        state_manager.push_state({
            "name": bucket_name,
            "crn": apply_tf_result["bucket_crn"]["value"],
            "virtual_server_endpoint": virtual_server_endpoint,
            "storage_class": payload.storage_class,
            "enable_custom_permissions": payload.enable_custom_permissions,
            "clean_status": Status.SUCCESS.value,
            "lifecycle_policy_rule_enabled": False,
            "immutability_choice": immutability["immutability_choice"],
            "retention": immutability["retention"],
            "object_locking_enabled": immutability["object_locking_enabled"],
            "object_lock_duration_days": immutability["object_lock_duration_days"],
            "object_lock_duration_years": immutability["object_lock_duration_years"],
            "enable_versioning": immutability["object_versioning_enabled"],
            "backup": immutability["backup"],
        })

        complete_bucket_create(payload.subscription_id, vip, apply_tf_result, session)
        return {"subscription_id": payload.subscription_id}

    validated = validate_request()
    immutability = process_protection_configuration(validated=validated)
    account_instances_crn = get_account_instances_crn(validated=validated)
    workspace_id = create_tf_workspace(
        validated=validated, account_instances_crn=account_instances_crn, immutability=immutability
    )
    apply_tf_result = apply_tf_workspace(
        validated=validated,
        workspace_id=workspace_id,
        account_instances_crn=account_instances_crn,
        immutability=immutability,
    )
    save_bucket_in_db(apply_tf_result=apply_tf_result, immutability=immutability)


bucket_create()
