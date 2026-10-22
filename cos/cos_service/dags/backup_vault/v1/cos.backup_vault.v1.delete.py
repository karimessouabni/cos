"""DAG cos.backup_vault.v1.delete : détruit les ressources Terraform d'un backup vault, puis son workspace.

Repris du fichier d'entreprise. Corrections par rapport à l'original : un
backup vault introuvable est refusé proprement (l'original faisait
``dict(None)`` puis indexait ``backup_vault["cos"]`` sur ``None`` avant même
le refus) ; un vault sans instance COS rattachée est refusé au lieu de planter
en ``KeyError`` ; la vérification des recovery ranges n'a lieu que si le
vault est complet. Les étapes de destruction tolèrent un workspace déjà
disparu côté Schematics (``code == 404``) et verrouillent le vault sur toute
autre erreur, comme pour le bucket.
"""
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
from cos_service.schemas.status import Status  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.subscription_status import SubscriptionStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

# Destruction des ressources : Schematics est interrogé toutes les 5 s,
# au plus 540 fois, soit 45 minutes.
DESTROY_POLL_DELAY_SECONDS = 5
DESTROY_MAX_ATTEMPTS = 540
SCHEMATICS_NOT_FOUND = 404


class CosBackupVaultDeletePayload(ProductActionPayload):
    pass


def _is_not_found(exc: Exception) -> bool:
    """Un workspace déjà supprimé côté Schematics remonte avec ``code == 404``."""
    return getattr(exc, "code", None) == SCHEMATICS_NOT_FOUND


def _mark_failed(subscription_id: str, session: SASession) -> None:
    from cos_service.services.backup_vault_service import update_backup_vault_status
    from cos_service.services.workspaceService import update_backup_vault_workspace_status

    update_backup_vault_status(subscription_id, SubscriptionStatus.LOCKED, session)
    update_backup_vault_workspace_status(subscription_id, Status.FAILED, session)


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=CosBackupVaultDeletePayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def backup_vault_delete():
    @step
    def validate_backup_vault_and_workspace(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: CosBackupVaultDeletePayload = depends(payload_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict:
        """Refuse la suppression d'un vault incomplet ou qui porte encore des recovery ranges.

        Renvoie la ligne du backup vault (relations incluses) pour les étapes suivantes.
        """
        from cos_service.services.backup_vault_service import (
            check_backup_vault_has_active_recovery_ranges,
            get_backup_vault_by_sub_id,
        )
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.vault_service import get_vault_secrets

        row = get_backup_vault_by_sub_id(payload.subscription_id, session)
        if row is None:
            raise DeclineDemandException(f"the backup vault doesn't exist for the sub id {payload.subscription_id}")
        backup_vault = dict(row)
        logger.info("backup vault %s: %s", payload.subscription_id, backup_vault)

        errors = []
        if not backup_vault.get("name"):
            errors.append(f"the backup vault is not fully created for the sub id {payload.subscription_id} (no name)")
        cos = backup_vault.get("cos")
        if not cos:
            errors.append("the backup vault is not linked to a cos instance")

        # --- recovery ranges : un vault qui en porte encore ne peut pas partir ---
        if not errors and cos:
            context = cos["context"]
            secrets = get_vault_secrets(
                realm_name=context["realm"], apcode=context["app_code"], vault=vault, reader=reader
            )
            access_token = get_iam_access_token(secrets["wklapp_account_api_key"])
            active_ranges, expired_ranges = check_backup_vault_has_active_recovery_ranges(access_token, backup_vault)
            if active_ranges:
                errors.append(
                    f"The backup vault {backup_vault['name']} still has active recovery ranges. You cannot delete it."
                )
            if expired_ranges:
                errors.append(
                    f"The backup vault {backup_vault['name']} still has some expired recovery ranges. "
                    "Wait for backend cleanup before deleting it."
                )

        if errors:
            raise DeclineDemandException(" | ".join(errors))

        return backup_vault

    @step
    def destroy_tf_resources(
        backup_vault: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        payload: CosBackupVaultDeletePayload = depends(payload_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> bool:
        from cos_service.services.backup_vault_service import update_backup_vault_on_destroy, update_backup_vault_status
        from cos_service.services.workspaceService import (
            build_backup_vault_workspace_details,
            update_backup_vault_workspace,
        )

        workspace_id = backup_vault["workspace"]["workspace_id"]
        cos = backup_vault["cos"]

        workspace_details = build_backup_vault_workspace_details(
            workspace_id=workspace_id,
            realm=cos["context"]["realm"],
            app_code=cos["context"]["app_code"],
            region=backup_vault["region"],
            cos_instance_crn=cos["crn"],
            cos_instance_id=cos["crn"],
            cos_instance_name=cos["name"],
            kms_crn=backup_vault["kms_crn"],
            description=backup_vault["description"],
        )

        try:
            # Pose action=DESTROY et status=INPROGRESS sur le workspace, puis
            # rafraîchit VCS, token GitLab et variables (tokens Vault à durée
            # de vie courte) : sans ça le provider ne s'authentifie pas au destroy.
            update_backup_vault_on_destroy(payload.subscription_id, session)
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.TERMINATING, session)
            update_backup_vault_workspace(workspace_details=workspace_details, vault=vault, tf=tf, reader=reader)
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
        backup_vault: dict,
        destroyed_resources: bool,
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from sqlalchemy import update

        from cos_service.models.BackupVault import BackupVault

        session.execute(
            update(BackupVault)
            .values(action=Action.DESTROY.value)
            .where(BackupVault.subscription_id == backup_vault["subscription_id"])
        )
        session.commit()
        logger.info("backup vault %s marked as destroyed", backup_vault["subscription_id"])
        return True

    @step
    def destroy_tf_workspace(
        backup_vault: dict,
        update_db_resources_task: bool,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.backup_vault_service import update_backup_vault_status
        from cos_service.services.workspaceService import update_backup_vault_workspace_status

        workspace_id = backup_vault["workspace"]["workspace_id"]
        if workspace_id is None:
            logger.info("backup vault %s has no workspace, nothing to delete", backup_vault["subscription_id"])
            return True

        try:
            update_backup_vault_status(backup_vault["subscription_id"], SubscriptionStatus.TERMINATING, session)
            update_backup_vault_workspace_status(backup_vault["subscription_id"], Status.INPROGRESS, session)
            tf.workspaces.get_by_id(workspace_id=workspace_id).delete()
            logger.info("workspace %s deleted", workspace_id)
        except Exception as exc:
            if not _is_not_found(exc):
                logger.error("deleting workspace %s failed: %s", workspace_id, exc)
                _mark_failed(backup_vault["subscription_id"], session)
                raise
            logger.info("workspace %s is already deleted", workspace_id)

        return True

    @step
    def update_db_for_workspace(
        backup_vault: dict,
        destroyed_workspace: bool,
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.backup_vault_service import update_backup_vault_status
        from cos_service.services.workspaceService import update_backup_vault_workspace_status

        update_backup_vault_status(backup_vault["subscription_id"], SubscriptionStatus.TERMINATED, session)
        update_backup_vault_workspace_status(backup_vault["subscription_id"], Status.SUCCESS, session)
        return True

    backup_vault = validate_backup_vault_and_workspace()
    destroyed_resources = destroy_tf_resources(backup_vault=backup_vault)
    update_db_resources_task = update_db_for_resources(backup_vault=backup_vault, destroyed_resources=destroyed_resources)
    destroyed_workspace = destroy_tf_workspace(backup_vault=backup_vault, update_db_resources_task=update_db_resources_task)
    update_db_for_workspace(backup_vault=backup_vault, destroyed_workspace=destroyed_workspace)


backup_vault_delete()
