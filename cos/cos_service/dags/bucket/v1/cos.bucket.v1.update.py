"""DAG cos.bucket.v1.update : met à jour la protection d'un bucket existant.

[RECONSTITUTION] Reconstitué depuis les captures PyCharm (259 lignes, toutes
visibles). Corrections par rapport à l'original, voir le commit : une seule
étape de validation en tête qui décline avec toutes les erreurs à la fois
(bucket, workspace, backup vault, contenu, règles d'immutabilité), plus de
relecture du vault ni de reconstruction du choix d'immutabilité dans le DAG,
plan/apply délégués à ``run_workspace``, ``raise`` nu, payload typé proprement.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

import logging
from pathlib import Path

from airflow.sensors.date_time import DateTimeSensorAsync  # Airflow 2.x

from bp2i_airflow_library.dag import product_action, step
from bp2i_airflow_library.dependencies import (
    SASession,
    SchematicsBackend,
    StateManager,
    Vault,
    depends,
    payload_dependency,
    smart_schematics_backend_dependency,
    sqlalchemy_session_dependency,
    state_manager_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from bp2i_airflow_library.schemas import Field, ProductActionPayload

from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.bucket_retention import BucketRetention
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus

logger = logging.getLogger(__name__)

SCHEDULING_TIMEZONE = "Europe/Paris"
# Défaut dans le passé : sans date dans le payload, le sensor ne bloque pas.
SCHEDULING_DEFAULT = "2025-06-03T12:13:02.000"


class BucketUpdatePayload(ProductActionPayload):
    retention: BucketRetention | None = Field(default=None, updatable=True)
    object_lock_duration_days: int | None = Field(default=None, updatable=True)
    object_lock_duration_years: int | None = Field(default=None, updatable=True)
    enable_custom_permissions: bool | None = Field(default=None, updatable=True)
    enable_versioning: bool | None = Field(default=None, updatable=True)
    backup: BucketBackup | None = Field(default=None, updatable=True)
    scheduling_update_date_time: str = Field(default=SCHEDULING_DEFAULT)


@product_action(Path(__file__).stem, tags=["cos"], payload=BucketUpdatePayload)
def bucket_update():
    @step
    def validate_request(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketUpdatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict:
        """Checks everything the update refers to and declines with all the errors at once.

        Returns {"bucket": <dict>, "immutability": <dict>, "backup_vault_crn": <str | None>,
        "enable_custom_permissions": <bool>} for the next steps.
        """
        from cos_service.services.backup_vault_service import get_backup_vault_by_name, get_backup_vault_by_sub_id
        from cos_service.services.bucketService import check_bucket_has_contents, get_bucket_by_sub_id
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.immutability_service import validate_immutability_for_update_bucket
        from cos_service.services.vault_service import get_cos_api_key

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        if bucket is None:
            raise DeclineDemandException(f"the bucket doesn't exist for the sub id {payload.subscription_id}")

        errors = []

        # --- the bucket must have been fully created --------------------------
        if not bucket["name"]:
            errors.append(f"the bucket is not fully created for the sub id {payload.subscription_id} (no name)")
        if not bucket["virtual_server_endpoint"]:
            errors.append("the bucket has no endpoint, its contents cannot be checked")
        workspace = bucket.get("workspace")
        if workspace is None or workspace.get("workspace_id") is None:
            errors.append("the bucket has no Terraform workspace, it cannot be updated")
        if not bucket.get("cos"):
            errors.append("the bucket is not linked to a cos instance")

        # --- backup vault (only when a backup is requested) -------------------
        backup = payload.backup
        if backup is not None and backup.backup_enabled is True:
            if not backup.backup_vault_name:
                errors.append("The Backup Vault name is required to enable bucket backup")
            else:
                backup_vault = get_backup_vault_by_name(backup.backup_vault_name, session)
                if backup_vault is None:
                    errors.append(f"No Backup Vault exist with the name : {backup.backup_vault_name}")
                else:
                    backup.backup_vault_sub_id = backup_vault["subscription_id"]

        # --- immutability rules, against the current bucket and its contents ---
        immutability = None
        if bucket["virtual_server_endpoint"]:
            access_token = get_iam_access_token(get_cos_api_key(bucket, vault))
            has_contents = check_bucket_has_contents(access_token, bucket)
            try:
                immutability = validate_immutability_for_update_bucket(
                    bucket,
                    payload.retention,
                    payload.object_lock_duration_days,
                    payload.object_lock_duration_years,
                    payload.enable_versioning,
                    has_contents,
                    backup,
                    session,
                )
            except DeclineDemandException as exc:
                # Le service joint déjà ses erreurs avec " | " : on les fusionne
                # dans la même liste pour un seul refus.
                errors.extend(str(exc).split(" | "))

        if errors:
            raise DeclineDemandException(" | ".join(errors))

        backup_vault_sub_id = immutability["backup"]["backup_vault_sub_id"]
        backup_vault_crn = (
            get_backup_vault_by_sub_id(backup_vault_sub_id, session).crn if backup_vault_sub_id is not None else None
        )
        # Seul champ hors immutability : repli sur la valeur déjà en base.
        enable_custom_permissions = (
            payload.enable_custom_permissions
            if payload.enable_custom_permissions is not None
            else bucket["enable_custom_permissions"]
        )

        logger.info("update of bucket %s validated: %s", bucket["name"], immutability)
        return {
            "bucket": bucket,
            "immutability": immutability,
            "backup_vault_crn": backup_vault_crn,
            "enable_custom_permissions": enable_custom_permissions,
        }

    @step
    def compute_target_time(payload: BucketUpdatePayload = depends(payload_dependency)) -> str:
        import pendulum

        logger.info("Execution is scheduled for %s", payload.scheduling_update_date_time)
        return pendulum.parse(payload.scheduling_update_date_time, tz=SCHEDULING_TIMEZONE).to_iso8601_string()

    @step
    def update_tf_workspace(
        validated: dict,
        state_manager: StateManager = depends(state_manager_dependency),
        payload: BucketUpdatePayload = depends(payload_dependency),
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.workspaceService import update_bucket_workspace, build_bucket_workspace_details
        from cos_service.services.bucketService import update_bucket_workspace_status, update_bucket_status
        from cos_service.services.schematics_service import run_workspace

        bucket = validated["bucket"]
        immutability = validated["immutability"]
        workspace_id = bucket["workspace"]["workspace_id"]

        workspace_details = build_bucket_workspace_details(
            workspace_id=workspace_id,
            realm=bucket["cos"]["context"]["realm"],
            app_code=bucket["cos"]["context"]["app_code"],
            region=bucket["region"],
            storage_class=bucket["storage_class"],
            cos_instance_crn=bucket["cos"]["crn"],
            cos_instance_name=bucket["cos"]["name"],
            activity_tracker_crn=bucket["activity_tracker_crn"],
            kms_crn=bucket["kms_crn"],
            monitoring_crn=bucket["monitoring_crn"],
            management_endpoint_type="direct",
            immutability=immutability,
            enable_custom_permissions=validated["enable_custom_permissions"],
            description=state_manager.get_subscription().description,
            backup_vault_crn=validated["backup_vault_crn"],
        )

        try:
            update_bucket_status(payload.subscription_id, SubscriptionStatus.ACTIVE, session)
            update_bucket_workspace_status(payload.subscription_id, Status.INPROGRESS, session)
            update_bucket_workspace(payload=payload, workspace_details=workspace_details, vault=vault, tf=tf)
            run_workspace(tf, workspace_id)
            return True
        except Exception:
            update_bucket_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_bucket_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def save_bucket_in_db(
        validated: dict,
        is_update_ws_done: bool,
        payload: BucketUpdatePayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict:
        from cos_service.services.bucketService import process_bucket_update, update_bucket_workspace_status
        from cos_service.services.immutability_service import retention_state_for_client

        immutability = validated["immutability"]
        enable_custom_permissions = validated["enable_custom_permissions"]

        # Le state reflète la configuration effective calculée par le service,
        # y compris le choix d'immutabilité qu'il a résolu.
        state_manager.push_state({
            # Clés historiques en jours conservées, unité et bornes saisies ajoutées.
            "retention": retention_state_for_client(payload.retention, immutability["retention"]),
            "object_lock_duration_days": immutability["object_lock_duration_days"],
            "object_lock_duration_years": immutability["object_lock_duration_years"],
            "object_locking_enabled": immutability["object_locking_enabled"],
            "enable_versioning": immutability["object_versioning_enabled"],
            "backup": immutability["backup"],
            "enable_custom_permissions": enable_custom_permissions,
            "immutability_choice": immutability["immutability_choice"],
        })

        process_bucket_update(
            payload.subscription_id,
            immutability,
            enable_custom_permissions,
            state_manager.get_subscription().description,
            session,
        )
        update_bucket_workspace_status(payload.subscription_id, Status.SUCCESS, session)
        return {"subscription_id": payload.subscription_id}

    validated = validate_request()
    target = compute_target_time()

    wait = DateTimeSensorAsync(
        task_id="wait_for_scheduled_time",
        target_time="{{ ti.xcom_pull(task_ids='%s') }}" % target.operator.task_id,
    )
    validated >> wait
    target >> wait

    is_update_ws_done = update_tf_workspace(validated=validated)
    wait >> is_update_ws_done

    save_bucket_in_db(validated=validated, is_update_ws_done=is_update_ws_done)


bucket_update()
