"""DAG cos.bucket.v1.delete : détruit les ressources Terraform, puis le workspace."""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import Context as AirflowContext  # noqa: F401
    from airflow.sdk import TriggerRule, task, task_group  # noqa: F401
else:
    from airflow.decorators import task, task_group  # noqa: F401, AIR301
    from airflow.utils.trigger_rule import TriggerRule  # noqa: F401, AIR301
    from airflow.utils.context import Context as AirflowContext  # noqa: F401, AIR301

import logging  # noqa: E402 - après add_project_to_path()
from pathlib import Path  # noqa: E402 - après add_project_to_path()

from bp2i_airflow_library.config import ENVIRONMENT  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.connectors.reader import ReaderConnector  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dag import product_action, step  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dependencies import (  # noqa: E402 - après add_project_to_path()
    SASession,
    SchematicsBackend,
    Vault,
    depends,
    payload_dependency,
    reader_dependency,
    smart_schematics_backend_dependency,
    sqlalchemy_session_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload  # noqa: E402 - après add_project_to_path()
from bp2i_terraform.components.cooldown_policies import LinearCooldownPolicy  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.action import Action  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.clean_status import CleanStatus  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.status import Status  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.subscription_status import SubscriptionStatus  # noqa: E402 - après add_project_to_path()

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
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=BucketDeletePayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def bucket_delete():
    @step
    def validate_request(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketDeletePayload = depends(payload_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        vault: Vault = depends(vault_dependency),
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
    ) -> dict:
        """Checks everything the deletion needs and declines with all the errors at once.

        Returns the bucket row (relations included) the next steps work on.
        The bucket moves to TERMINATING only once every check has passed.

        A clean scheduled or running owns the bucket: declined. A leftover
        quarantine (failed or interrupted clean, quarantine test) blocks every
        access to the bucket, the content check and the destroy included: it is
        lifted first (ADR 0004).
        """
        from cos_service.services.bucketService import (
            check_bucket_has_contents,
            get_bucket_by_sub_id,
            update_bucket_status,
        )
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.vault_service import get_cos_api_key

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        if bucket is None:
            raise DeclineDemandException(f"the bucket doesn't exist for the sub id {payload.subscription_id}")
        if bucket.get("clean_status") in [s.value for s in CleanStatus.busy()]:
            raise DeclineDemandException(
                f"a clean of the bucket {bucket['name']} is {bucket['clean_status']}: "
                "cancel it (cos.bucket.v1.cancel_clean) or wait for its end before deleting the bucket"
            )
        if bucket.get("clean_cbr_workspace_id"):
            from cos_service.services.bucketService import set_bucket_clean_workspace
            from cos_service.services.quarantine_service import lift_bucket_quarantine

            logger.info("lifting the leftover quarantine of %s before the deletion", bucket["name"])
            lift_bucket_quarantine(tf=tf, workspace_id=bucket["clean_cbr_workspace_id"])
            set_bucket_clean_workspace(bucket["subscription_id"], None, session)
            bucket = {**bucket, "clean_cbr_workspace_id": None}

        logger.info(
            "bucket %s: workspace %r, cos loaded: %s, backup_vault %r",
            payload.subscription_id,
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
        # get_bucket_by_sub_id garantit la clé "workspace" (relue dans sa table
        # quand to_dict() ne la fournit pas) : None = aucune ligne rattachée.
        workspace = bucket.get("workspace")
        if workspace is None:
            errors.append("no workspace row is attached to this bucket, its resources cannot be destroyed")
        elif workspace.get("workspace_id") is None:
            errors.append("the bucket workspace has no Schematics id, its resources cannot be destroyed")
        if not bucket.get("cos"):
            errors.append("the bucket is not linked to a cos instance")

        # --- the bucket must be empty (only reachable through its endpoint) ----
        has_contents = False
        if bucket["virtual_server_endpoint"]:
            access_token = get_iam_access_token(get_cos_api_key(bucket, vault, reader))
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
        reader: ReaderConnector = depends(reader_dependency),
        payload: BucketDeletePayload = depends(payload_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> bool:
        from cos_service.services.bucketService import (
            update_bucket_on_destroy,
            update_bucket_status,
            update_bucket_workspace_status,
        )
        from cos_service.services.immutability_service import compute_bucket_immutability_for_update_bucket
        from cos_service.services.workspaceService import build_bucket_workspace_details, update_bucket_workspace

        # Pose action=DESTROY et status=INPROGRESS sur le workspace.
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
            # Rafraîchit VCS, token GitLab et variables (dont les tokens Vault,
            # à durée de vie courte) : sans ça le provider ne s'authentifie
            # pas au destroy. Les valeurs métier, elles, ne servent à rien ici.
            update_bucket_status(payload.subscription_id, SubscriptionStatus.TERMINATING, session)
            update_bucket_workspace_status(payload.subscription_id, Status.INPROGRESS, session)
            update_bucket_workspace(
                payload=payload, workspace_details=workspace_details, vault=vault, tf=tf, reader=reader
            )
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
