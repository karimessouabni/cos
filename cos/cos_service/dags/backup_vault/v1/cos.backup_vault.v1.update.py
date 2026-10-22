"""DAG cos.backup_vault.v1.update : rejoue le workspace Terraform d'un backup vault.

Repris du fichier d'entreprise : seule la description de la souscription est
mise à jour (workspace Schematics rafraîchi, plan puis apply, puis la ligne en
base). Un vault introuvable ou sans nom est refusé.
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
    StateManager,
    Vault,
    depends,
    payload_dependency,
    reader_dependency,
    smart_schematics_backend_dependency,
    sqlalchemy_session_dependency,
    state_manager_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.schemas import ProductActionPayload  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.status import Status  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.subscription_status import SubscriptionStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)


class CosBackupVaultUpdatePayload(ProductActionPayload):
    pass


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=CosBackupVaultUpdatePayload,
)
def backup_vault_update():
    @step
    def validate_backup_vault(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: CosBackupVaultUpdatePayload = depends(payload_dependency),
    ) -> dict:
        from cos_service.services.backup_vault_service import get_backup_vault_by_sub_id

        row = get_backup_vault_by_sub_id(payload.subscription_id, session)
        backup_vault = dict(row) if row is not None else None
        if backup_vault is None or not backup_vault.get("name"):
            raise DeclineDemandException(
                f"The backup vault doesn't exist or is not fully created for the sub id {payload.subscription_id}."
            )
        return backup_vault

    @step
    def update_tf_workspace(
        backup_vault: dict,
        state_manager: StateManager = depends(state_manager_dependency),
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        payload: CosBackupVaultUpdatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.backup_vault_service import update_backup_vault_status
        from cos_service.services.workspaceService import (
            build_backup_vault_workspace_details,
            update_backup_vault_workspace,
            update_backup_vault_workspace_status,
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
            description=state_manager.get_subscription().description,
        )

        try:
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.ACTIVE, session)
            update_backup_vault_workspace_status(payload.subscription_id, Status.INPROGRESS, session)
            update_backup_vault_workspace(workspace_details=workspace_details, vault=vault, tf=tf, reader=reader)

            tf_workspace = tf.workspaces.get_by_id(workspace_id)
            tf_workspace.plan()
            apply_activity = tf_workspace.apply()

            for template_id, template_activity_logs in apply_activity.get_logs().items():
                logger.info("%s\n%s", template_id, template_activity_logs)
            tf_workspace.get_outputs()
            return True
        except Exception:
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_backup_vault_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def update_backup_vault_in_db(
        apply_tf_result: bool,
        payload: CosBackupVaultUpdatePayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict | None:
        from cos_service.services.backup_vault_service import process_backup_vault_update
        from cos_service.services.workspaceService import update_backup_vault_workspace_status

        description = state_manager.get_subscription().description
        process_backup_vault_update(payload.subscription_id, description, session)
        update_backup_vault_workspace_status(payload.subscription_id, Status.SUCCESS, session)
        return {"subscription_id": payload.subscription_id}

    backup_vault = validate_backup_vault()
    apply_tf_result = update_tf_workspace(backup_vault=backup_vault)
    update_backup_vault_in_db(apply_tf_result=apply_tf_result)


backup_vault_update()
