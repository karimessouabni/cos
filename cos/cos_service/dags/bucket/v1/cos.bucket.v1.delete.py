"""DAG cos.bucket.v1.delete : détruit les ressources Terraform, puis le workspace.

[RECONSTITUTION] Reconstitué depuis les captures PyCharm (273 lignes, toutes
visibles). Corrections par rapport à l'original, voir les commits : une seule
étape de validation en tête (bucket, workspace, instance, contenu) qui décline
avec toutes les erreurs à la fois, l'immutabilité relue par le service au lieu
d'être recomposée à la main, la tolérance au 404 Schematics factorisée, plus de
SQL brut dans le DAG, statuts posés une fois, ``raise`` nu.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

import logging
from pathlib import Path

from bp2i_airflow_library.dag import product_action, step
from bp2i_airflow_library.dependencies import (
    SASession,
    SchematicsBackend,
    Vault,
    depends,
    payload_dependency,
    smart_schematics_backend_dependency,
    sqlalchemy_session_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload
from bp2i_terraform.components.cooldown_policies import LinearCooldownPolicy

from cos_service.schemas.action import Action
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus

logger = logging.getLogger(__name__)

# Destruction des ressources : Schematics est interrogé toutes les 5 s,
# au plus 540 fois, soit 45 minutes.
DESTROY_POLL_DELAY_SECONDS = 5
DESTROY_MAX_ATTEMPTS = 540
SCHEMATICS_NOT_FOUND = 404


class BucketDeletePayload(ProductActionPayload):
    pass


def _is_not_found(exc: Exception) -> bool:
    """Un workspace déjà supprimé côté Schematics remonte avec ``code == 404``."""
    return getattr(exc, "code", None) == SCHEMATICS_NOT_FOUND


def _mark_failed(subscription_id: str, session: SASession) -> None:
    from cos_service.services.bucketService import update_bucket_status, update_bucket_workspace_status

    update_bucket_status(subscription_id, SubscriptionStatus.LOCKED, session)
    update_bucket_workspace_status(subscription_id, Status.FAILED, session)


@product_action(
    Path(__file__).stem,
    tags=["cos"],
    payload=BucketDeletePayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def bucket_delete():
    @step
    def validate_request(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketDeletePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict:
        """Checks everything the deletion needs and declines with all the errors at once.

        Returns the bucket row (relations included) the next steps work on.
        The bucket moves to TERMINATING only once every check has passed.
        """
        from cos_service.services.bucketService import (
            check_bucket_has_contents,
            get_bucket_by_sub_id,
            get_bucket_workspace,
            update_bucket_status,
        )
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.vault_service import get_cos_api_key

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        if bucket is None:
            raise DeclineDemandException(f"the bucket doesn't exist for the sub id {payload.subscription_id}")

        # Ce que to_dict() a réellement sérialisé : permet de distinguer une
        # relation absente du dict d'un workspace sans id en base.
        logger.info(
            "bucket %s keys: %s, workspace: %r, cos loaded: %s, backup_vault: %r",
            payload.subscription_id,
            sorted(bucket),
            bucket.get("workspace"),
            bucket.get("cos") is not None,
            bucket.get("backup_vault"),
        )

        errors = []

        # --- the bucket must have been fully created --------------------------
        if not bucket["name"]:
            errors.append(f"the bucket is not fully created for the sub id {payload.subscription_id} (no name)")
        if not bucket["virtual_server_endpoint"]:
            errors.append("the bucket has no endpoint, its contents cannot be checked")
        # Le workspace est relu dans sa table quand to_dict() ne l'a pas fourni,
        # pour ne pas dépendre de la sérialisation des relations du modèle.
        workspace = bucket.get("workspace")
        if workspace is None:
            workspace = get_bucket_workspace(session, payload.subscription_id)
            logger.info("workspace read from its table for %s: %r", payload.subscription_id, workspace)
            bucket["workspace"] = workspace
        if workspace is None:
            errors.append("no workspace row is attached to this bucket, its resources cannot be destroyed")
        elif workspace.get("workspace_id") is None:
            errors.append("the bucket workspace has no Schematics id, its resources cannot be destroyed")
        if not bucket.get("cos"):
            errors.append("the bucket is not linked to a cos instance")

        # --- the bucket must be empty (only reachable through its endpoint) ----
        has_contents = False
        if bucket["virtual_server_endpoint"]:
            access_token = get_iam_access_token(get_cos_api_key(bucket, vault))
            has_contents = check_bucket_has_contents(access_token, bucket)
            if has_contents:
                errors.append(f"The bucket {bucket['name']} is not empty")

        if errors:
            if has_contents:
                update_bucket_status(bucket["subscription_id"], SubscriptionStatus.LOCKED, session)
            raise DeclineDemandException(" | ".join(errors))

        update_bucket_status(bucket["subscription_id"], SubscriptionStatus.TERMINATING, session)
        logger.info("deleting bucket %s (subscription %s)", bucket["name"], payload.subscription_id)
        return bucket

    @step
    def destroy_tf_resources(
        bucket: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        payload: BucketDeletePayload = depends(payload_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> bool:
        from cos_service.services.workspaceService import update_bucket_workspace, build_bucket_workspace_details
        from cos_service.services.bucketService import update_bucket_on_destroy, update_bucket_workspace_status
        from cos_service.services.immutability_service import compute_bucket_immutability_for_update_bucket

        update_bucket_on_destroy(payload.subscription_id, session)

        workspace_id = bucket["workspace"]["workspace_id"]
        # Même bloc que celui de l'update : l'original le recomposait à la main
        # sans object_lock_duration_years et en relisant le vault en base.
        immutability = compute_bucket_immutability_for_update_bucket(bucket, session)
        backup_vault = bucket.get("backup_vault")

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
            enable_custom_permissions=bucket["enable_custom_permissions"],
            description=bucket["description"],
            backup_vault_crn=backup_vault["crn"] if backup_vault is not None else None,
        )

        try:
            update_bucket_workspace_status(payload.subscription_id, Status.INPROGRESS, session)
            update_bucket_workspace(payload=payload, workspace_details=workspace_details, vault=vault, tf=tf)
            tf.workspaces.get_by_id(workspace_id=workspace_id)  # lève avec code 404 si déjà supprimé
            tf.workspaces.delete_workspace_resources(
                workspace_id,
                cooldown_policy=LinearCooldownPolicy(
                    delay=DESTROY_POLL_DELAY_SECONDS, max_attempts=DESTROY_MAX_ATTEMPTS
                ),
            )
            logger.info("resources of workspace %s destroyed", workspace_id)
        except Exception as exc:
            if not _is_not_found(exc):
                logger.error("destroying resources of workspace %s failed: %s", workspace_id, exc)
                _mark_failed(payload.subscription_id, session)
                raise
            logger.info("workspace %s not found, its resources are considered destroyed", workspace_id)

        return True

    @step
    def update_db_for_resources(
        bucket: dict,
        destroyed_resources: bool,
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import update_bucket_action

        update_bucket_action(bucket["subscription_id"], Action.DESTROY, session)
        return True

    @step
    def destroy_tf_workspace(
        bucket: dict,
        update_db_resources_task: bool,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import update_bucket_workspace_status

        workspace_id = bucket["workspace"]["workspace_id"]
        try:
            update_bucket_workspace_status(bucket["subscription_id"], Status.INPROGRESS, session)
            tf.workspaces.get_by_id(workspace_id=workspace_id).delete()
            logger.info("workspace %s deleted", workspace_id)
        except Exception as exc:
            if not _is_not_found(exc):
                logger.error("deleting workspace %s failed: %s", workspace_id, exc)
                _mark_failed(bucket["subscription_id"], session)
                raise
            logger.info("workspace %s is already deleted", workspace_id)

        return True

    @step
    def update_db_for_workspace(
        bucket: dict,
        destroyed_workspace: bool,
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import update_bucket_status, update_bucket_workspace_status

        update_bucket_status(bucket["subscription_id"], SubscriptionStatus.TERMINATED, session)
        update_bucket_workspace_status(bucket["subscription_id"], Status.SUCCESS, session)
        return True

    bucket = validate_request()
    destroyed_resources = destroy_tf_resources(bucket=bucket)
    update_db_resources_task = update_db_for_resources(bucket=bucket, destroyed_resources=destroyed_resources)
    destroyed_workspace = destroy_tf_workspace(bucket=bucket, update_db_resources_task=update_db_resources_task)
    update_db_for_workspace(bucket=bucket, destroyed_workspace=destroyed_workspace)


bucket_delete()
