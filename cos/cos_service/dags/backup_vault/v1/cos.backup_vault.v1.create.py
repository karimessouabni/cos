"""DAG cos.backup_vault.v1.create : crée un backup vault sur une instance COS.

Repris du fichier d'entreprise. Deux corrections par rapport à l'original :
une instance COS introuvable est refusée proprement (l'original lisait
``cos_instance.name`` sur ``None`` et plantait avant le refus) ; et l'échec de
création du workspace pose ``Status.FAILED`` via
``update_backup_vault_workspace_status`` (l'original appelait
``update_backup_vault_workspace_details`` avec un statut à la place des
détails du workspace).
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
from bp2i_airflow_library.schemas import Field, ProductCreatePayload  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.status import Status  # noqa: E402 - après add_project_to_path()
from cos_service.schemas.subscription_status import SubscriptionStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

# Un backup vault vit dans la région du produit, fixée.
BACKUP_VAULT_REGION = "eu-fr2"
TF_DIRECTORY = "terraform/v1.12/backup_vault"


class CosBackupVaultCreatePayload(ProductCreatePayload):
    cos_instance: str = Field(default="co21000001", minLength=0, maxLength=20)


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=CosBackupVaultCreatePayload,
)
def backup_vault_create():
    @step
    def validate_context_realm_apcode(
        payload: CosBackupVaultCreatePayload = depends(payload_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
    ) -> dict:
        """Vérifie realm, apcode et instance COS, et refuse avec toutes les erreurs à la fois.

        Renvoie la ligne de l'instance COS (dict) pour les étapes suivantes.
        """
        from cos_service.services.contextService import get_apcodes, get_realm
        from cos_service.services.cosService import get_cos_instance_by_name, get_cos_instance_status

        errors = []

        # --- realm / apcode ---------------------------------------------------
        realm = get_realm(reader)
        if realm is None or not payload.realm:
            errors.append(f"the realm {payload.realm} doesn't exist or is empty")
        else:
            raw_status = realm.status
            logger.info(" raw_status " + str(raw_status))
            if str(raw_status) == "404":
                errors.append(f"the realm {payload.realm} is not found")
            if realm.realm_apcode_details is None:
                errors.append(f"there is no apcodes on this realm {payload.realm}")
            elif payload.apcode not in get_apcodes(realm):
                errors.append(f"appCode {payload.apcode} doesn't belong to this realm {payload.realm}")

        # --- cos instance -----------------------------------------------------
        state_manager.push_state({"cos_instance": payload.cos_instance})
        cos_instance = get_cos_instance_by_name(payload.cos_instance, session)
        if cos_instance is None:
            errors.append(f"the cos instance {payload.cos_instance} doesn't exist")
        else:
            cos_instance_status = get_cos_instance_status(cos_instance.name, reader)
            if cos_instance_status != SubscriptionStatus.ACTIVE.value:
                errors.append(f"Bad cos instance status : {cos_instance_status}")

        if errors:
            raise DeclineDemandException(" | ".join(errors))

        return dict(cos_instance)

    @step
    def get_account_instances_crn(
        cos_instance: dict,
        reader: ReaderConnector = depends(reader_dependency),
    ) -> dict:
        from cos_service.services.contextService import get_account_instances_crn

        return get_account_instances_crn(reader)

    @step
    def create_tf_workspace(
        account_instances_crn: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        payload: CosBackupVaultCreatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> str:
        from bp2i_terraform.backends.schematics import TerraformVar
        from cos_service.services.backup_vault_service import (
            get_backup_vault_by_sub_id,
            process_backup_vault_creation,
            update_backup_vault_status,
        )
        from cos_service.services.contextService import get_realm
        from cos_service.services.cosService import get_cos_instance_by_name
        from cos_service.services.schematics_service import create_or_update_ws
        from cos_service.services.vault_service import get_vault_secrets
        from cos_service.services.workspaceService import (
            update_backup_vault_workspace_details,
            update_backup_vault_workspace_status,
        )

        realm = get_realm(reader)
        secrets = get_vault_secrets(realm_name=payload.realm, apcode=payload.apcode, vault=vault, reader=reader)
        ws_name = f"ws_backup_vault_{payload.subscription_id}"

        cos_instance = get_cos_instance_by_name(payload.cos_instance, session)

        variables = {
            "region": BACKUP_VAULT_REGION,
            "cos_instance_id": cos_instance.crn,
            "cos_instance_name": cos_instance.name,
            "kms_key_crn": account_instances_crn.get("encryption_key", None),
            "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
            "vault_read_addr": secrets["vault_read_addr"],
            "app_code": payload.apcode,
            "orchestrator_environment": ENVIRONMENT,
            "wklapp_account_id": realm.wklapp_account_number,
        }

        backup_vault = get_backup_vault_by_sub_id(payload.subscription_id, session)
        description = state_manager.get_subscription().description

        if backup_vault is None:
            backup_vault = process_backup_vault_creation(
                payload, account_instances_crn, description, cos_instance, session
            )

        try:
            if backup_vault["workspace"]["workspace_id"] is None:
                logger.info("Terraform create workspace")
                create_ws_result = create_or_update_ws(
                    tf,
                    ws_name,
                    ENVIRONMENT,
                    TF_DIRECTORY,
                    variables,
                    description,
                    secrets["gitlab_token"],
                )

                update_backup_vault_status(payload.subscription_id, SubscriptionStatus.CREATING, session)
                update_backup_vault_workspace_details(backup_vault, create_ws_result, session)
                state_manager.push_state({"workspace_name": ws_name})
                return create_ws_result["id"]

            logger.info("Workspace already created")
            return backup_vault["workspace"]["workspace_id"]

        except Exception:
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_backup_vault_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def apply_tf_workspace(
        workspace_id: str,
        account_instances_crn: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: CosBackupVaultCreatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict:
        from cos_service.services.backup_vault_service import update_backup_vault_status
        from cos_service.services.cosService import get_cos_instance_by_name
        from cos_service.services.workspaceService import (
            build_backup_vault_workspace_details,
            update_backup_vault_workspace,
            update_backup_vault_workspace_status,
        )

        cos_instance = get_cos_instance_by_name(payload.cos_instance, session)

        workspace_details = build_backup_vault_workspace_details(
            workspace_id=workspace_id,
            realm=payload.realm,
            app_code=payload.apcode,
            region=BACKUP_VAULT_REGION,
            cos_instance_crn=cos_instance.crn,
            cos_instance_id=cos_instance.crn,
            cos_instance_name=cos_instance.name,
            kms_crn=account_instances_crn.get("encryption_key", None),
            description=state_manager.get_subscription().description,
        )

        try:
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.CREATING, session)
            update_backup_vault_workspace_status(payload.subscription_id, Status.INPROGRESS, session)

            update_backup_vault_workspace(workspace_details=workspace_details, vault=vault, tf=tf, reader=reader)

            tf_workspace = tf.workspaces.get_by_id(workspace_id)
            tf_workspace.plan()
            apply_activity = tf_workspace.apply()

            for template_id, template_activity_logs in apply_activity.get_logs().items():
                logger.info("%s\n%s", template_id, template_activity_logs)
            outputs_tf = tf_workspace.get_outputs()[0]
            return dict(outputs_tf.output_values[0])
        except Exception:
            update_backup_vault_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            update_backup_vault_workspace_status(payload.subscription_id, Status.FAILED, session)
            raise

    @step
    def save_backup_vault_in_db(
        apply_tf_result: dict,
        payload: CosBackupVaultCreatePayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict | None:
        from cos_service.services.backup_vault_service import complete_backup_vault_create

        ws_state = {
            "name": apply_tf_result["backup_vault_name"]["value"],
            "crn": apply_tf_result["backup_vault_crn"]["value"],
            "id": apply_tf_result["backup_vault_id"]["value"],
            "cos_instance": payload.cos_instance,
        }
        state_manager.push_state(ws_state)

        complete_backup_vault_create(payload.subscription_id, apply_tf_result, session)
        return {"subscription_id": payload.subscription_id}

    cos_instance = validate_context_realm_apcode()
    account_instances_crn = get_account_instances_crn(cos_instance=cos_instance)
    workspace_id = create_tf_workspace(account_instances_crn=account_instances_crn)
    apply_tf_result = apply_tf_workspace(workspace_id=workspace_id, account_instances_crn=account_instances_crn)
    save_backup_vault_in_db(apply_tf_result=apply_tf_result)


backup_vault_create()
