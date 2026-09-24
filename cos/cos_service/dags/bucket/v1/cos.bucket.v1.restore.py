# ---------------------------------------------------------------
# 1  Imports de la bibliothèque standard
# ---------------------------------------------------------------
# [RECONSTITUTION] Reconstitué depuis les captures PyCharm (373 lignes, toutes
# visibles). Un seul changement fonctionnel : le point de restauration est une
# entrée du client (``restore_point_in_time``), vérifié dans [start, end] du
# recovery range choisi, au lieu de prendre le ``range_end_time`` du range le
# plus récent. Voir cos_service/services/recovery_range_service.py.
# Les autres points relevés dans l'analyse (nettoyage du workspace, token IAM
# en XCom, région en dur, mark_complete à l'initiation) sont laissés tels quels.
from __future__ import annotations

import json
import logging
import time
from os import access
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

# ---------------------------------------------------------------
# 2  Ajustement du PYTHONPATH (doit être fait avant
#    les imports qui dépendent du chemin ajouté)
# ---------------------------------------------------------------
from bp2i_airflow_library import add_project_to_path

add_project_to_path()  # <-- exécuté immédiatement

# ---------------------------------------------------------------
# 3  Imports de tiers / packages externes
# ---------------------------------------------------------------
from airflow.sensors.base import PokeReturnValue

# ---------------------------------------------------------------
# 4  Imports de projet (bp2i_airflow_library)
# ---------------------------------------------------------------
from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_airflow_library.dag import product_action, step
from bp2i_airflow_library.dependencies import (
    Vault,
    StateManager,
    depends,
    payload_dependency,
    state_manager_dependency,
    airflow_context_dependency,
    sqlalchemy_session_dependency,
    smart_schematics_backend_dependency,
    SchematicsBackend,
    vault_dependency,
    SASession,
)
from bp2i_airflow_library.schemas import Field, ProductActionPayload, ProductActionConfig
from cos_service.schemas.subscription_status import SubscriptionStatus
from cos_service.schemas.status import Status

# ---------------------------------------------------------------
# 5  Imports de projet (cos_service)
# ---------------------------------------------------------------
from cos_service.models.BackupVault import BackupVault
from cos_service.models.BackupVaultRestore import BackupVaultRestore
from cos_service.schemas.restore_status import RestoreStatus

logger = logging.getLogger(__name__)


class BucketRestoreBackupVaultPayload(ProductActionPayload):
    backup_vault_name: str = Field(min_length=3, max_length=63)
    target_bucket: str = Field(min_length=10, max_length=63)
    app_code: str = Field(min_length=3, max_length=63)
    realm: str = Field(min_length=3, max_length=63)
    # Point de restauration, ISO 8601 (ex. "2026-09-15T10:30:00Z"). Sans fuseau,
    # lu comme UTC. Doit tomber dans [range_start_time, range_end_time] du
    # recovery range utilisé.
    restore_point_in_time: str = Field(min_length=10, max_length=40, updatable=True)
    recovery_range_id: Optional[str] | None = Field(default=None, updatable=True)


@product_action(Path(__file__).stem, tags=["cos"],
                payload=BucketRestoreBackupVaultPayload, config=ProductActionConfig(
                    lock_subscription_on_failure=True
                ))
def bucket_restore_backup_vault():
    @step
    def input_user_validation(
            payload: BucketRestoreBackupVaultPayload = depends(payload_dependency),
            session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        from cos_service.services.bucketService import get_bucket_by_sub_id
        from cos_service.services.bucketService import get_bucket_by_name
        from cos_service.services.backup_vault_service import get_backup_vault_by_name
        from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
        from cos_service.services.recovery_range_service import RecoveryRangeError, parse_time

        errors: List[str] = []

        source_bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        logger.info("bucket %s", source_bucket)
        if source_bucket is None or not source_bucket["name"] or not source_bucket["bucket_crn"]:
            errors.append(
                f"The source bucket doesn't exist or is not fully created for subscription_id "
                f"{payload.subscription_id}."
            )
        if source_bucket["clean_status"] == RestoreStatus.RUNNING.value:
            errors.append(
                "A clean-bucket operation on source bucket is already ongoing. Restore cannot be started meanwhile."
            )
        if source_bucket.get("object_versioning_enabled") == False:
            errors.append(
                "The source bucket must have versioning enabled."
            )

        # validating target bucket bucket
        target_bucket = get_bucket_by_name(session, payload.target_bucket)
        if target_bucket is None or not target_bucket["name"] or not target_bucket["bucket_crn"]:
            errors.append(
                f"The target bucket doesn't exist or is not fully created for subscription_id "
                f"{payload.subscription_id}."
            )
        if target_bucket["clean_status"] == RestoreStatus.RUNNING.value:
            errors.append(
                "A clean-bucket operation on target bucket is already ongoing. Restore cannot be started meanwhile."
            )
        # ---------------------------------------------------------------
        # Vérification du backup vault
        # ---------------------------------------------------------------
        backup_vault = get_backup_vault_by_name(payload.backup_vault_name, session)
        if backup_vault is None:
            errors.append(
                f"The backup vault '{payload.backup_vault_name}' does not exist."
            )
        # ---------------------------------------------------------------
        # Point de restauration : format vérifié ici, appartenance à un
        # range vérifiée dans select_recovery_range.
        # ---------------------------------------------------------------
        try:
            parse_time(payload.restore_point_in_time)
        except RecoveryRangeError as exc:
            errors.append(str(exc))

        if errors:
            global_message = " | ".join(errors)
            raise DeclineDemandException(global_message)

        return {
            "target_bucket": target_bucket,
            "source_bucket": source_bucket,
            "backup_vault": backup_vault,
        }

    @step
    def validate_backup_vault(
            session: SASession = depends(sqlalchemy_session_dependency),
            payload: BucketRestoreBackupVaultPayload = depends(payload_dependency)
    ) -> dict:
        from cos_service.services.backup_vault_service import get_backup_vault_by_name

        backap_vault = get_backup_vault_by_name(payload.backup_vault_name, session)

        return dict(backap_vault)

    # ---------------------------------------------------------------
    # Ajout d'un step dédié à la récupération du token IAM
    # ---------------------------------------------------------------
    @step
    def get_wklapp_iam_token(
            payload: BucketRestoreBackupVaultPayload = depends(payload_dependency),
            vault: Vault = depends(vault_dependency)
    ) -> str:

        """
        Récupère le token IAM à partir du secret
        `wklapp_account_api_key` et le rend disponible pour les
        étapes suivantes.
        """
        from cos_service.services.vault_service import get_vault_secrets
        from cos_service.services.ibm_iam_service import get_iam_access_token

        # 1  Récupération du secret dans le vault
        secrets = get_vault_secrets(
            realm=payload.realm,
            apcode=payload.app_code,
            vault=vault,
        )

        # 2  Génération du token IAM
        iam_token = get_iam_access_token(secrets["wklapp_account_api_key"])
        return iam_token

    @step
    def select_recovery_range(
            iam_token,
            input_user_validation: dict,
            payload: BucketRestoreBackupVaultPayload = depends(payload_dependency),
    ) -> dict:
        """Range qui couvre le point de restauration demandé.

        Remplace l'ancien ``get_latest_recovery_range`` qui restaurait au
        ``range_end_time`` du range le plus récent : le point est maintenant
        une entrée du client et doit tomber dans [start, end] du range.
        Renvoie {"recovery_range": <range>, "restore_point_in_time": <ISO UTC>}.
        """
        from cos_service.services.restore_service import list_recovery_ranges
        from cos_service.services.recovery_range_service import (
            RecoveryRangeError,
            format_time,
            parse_time,
            select_recovery_range as choose,
        )
        from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException

        source_bucket = input_user_validation["source_bucket"]
        ranges = list_recovery_ranges(
            payload.backup_vault_name, source_bucket["bucket_crn"], iam_token)

        logger.info("recovery_ranges count=%d for bucket=%s", len(ranges), source_bucket["name"])

        try:
            restore_point = parse_time(payload.restore_point_in_time)
            recovery_range = choose(ranges, restore_point, payload.recovery_range_id)
        except RecoveryRangeError as exc:
            raise DeclineDemandException(f"{exc} (bucket {source_bucket['name']}, vault {payload.backup_vault_name})")

        restore_point_in_time = format_time(restore_point)
        logger.info(
            "restore point %s covered by recovery range %s [%s -> %s]",
            restore_point_in_time,
            recovery_range["recovery_range_id"],
            recovery_range["range_start_time"],
            recovery_range["range_end_time"],
        )
        return {"recovery_range": recovery_range, "restore_point_in_time": restore_point_in_time}

    @step
    def create_tf_workspace_and_launch_restore(
            iam_token: str,
            input_user_validation: dict,
            restore_target: Dict[str, Any],
            tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
            state_manager: StateManager = depends(state_manager_dependency),
            session: SASession = depends(sqlalchemy_session_dependency),
            payload: BucketRestoreBackupVaultPayload = depends(payload_dependency),
            vault: Vault = depends(vault_dependency),
    ) -> str:
        from bp2i_terraform.backends.schematics import TerraformVar
        from cos_service.services.schematics_service import create_or_update_ws
        from cos_service.repository.backup_vault_restore_repository import (
            create_restore
        )
        from cos_service.services.vault_service import get_vault_secrets
        from cos_service.services.contextService import get_realm
        from cos_service.repository.backup_vault_restore_repository import mark_complete, mark_failed

        restore_id = None
        target_bucket = input_user_validation["target_bucket"]
        backup_vault = input_user_validation["backup_vault"]
        recovery_range = restore_target["recovery_range"]
        restore_point_in_time = restore_target["restore_point_in_time"]
        realm = get_realm(payload.realm)
        secrets = get_vault_secrets(realm=realm.get("name", None), apcode=payload.app_code, vault=vault)
        ws_name = f"ws_bucket_restore_{payload.subscription_id}"
        tf_directory = "terraform/v1.12/backup_vault_restore"
        restore_payload = {
            "recovery_range_id": recovery_range["recovery_range_id"],
            "restore_point_in_time": restore_point_in_time,
            "restore_type": "in_place",
            "target_resource_crn": target_bucket["bucket_crn"],
        }
        logger.info('restore_payload: %s', restore_payload)

        variables = {
            "region": "eu-fr2",
            "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
            "vault_read_addr": secrets["vault_read_addr"],
            "app_code": payload.app_code,
            "orchestrator_environment": ENVIRONMENT,
            "wklapp_account_id": realm.get("wklapp_account_number", None),
            "access_token": 'Bearer ' + iam_token,
            "backup_vault_name": payload.backup_vault_name,
            "recovery_range_id": recovery_range["recovery_range_id"],
            "restore_point_in_time": restore_point_in_time,
            "restore_type": "in_place",
            "target_resource_crn": target_bucket["bucket_crn"]
        }

        description = state_manager.get_subscription().description

        try:
            if True:  # restore_backup_vault["workspace"]["workspace_id"] is None:
                logger.info("Terraform create workspace")
                stale_list = []
                ws_id_created = None  # <- initialisé ici, utilisé par le finally
                stale_ws = None
                try:
                    stale_list = tf.workspaces.get_by_name(ws_name) or []
                except Exception as ex:
                    if getattr(ex, "code", None) == 404:
                        stale_ws = []  # cas nominal : rien à purger
                    else:
                        raise

                for stale_ws in stale_list:
                    logger.info("stale workspace %s", stale_ws)
                    stale_ws.delete()
                    for _ in range(60):  # ~5 min max
                        try:
                            list_ws = tf.workspaces.get_by_name(ws_name)
                            if list_ws:
                                time.sleep(5)
                            else:
                                break
                        except Exception as ex:
                            if getattr(ex, "code", None) == 404:
                                logger.info("stale workspace fully deleted")
                                break
                            raise
                    else:
                        raise TimeoutError(f"workspace {ws_name} still present after delete")

                create_ws_result = create_or_update_ws(
                    tf,
                    ws_name,
                    ENVIRONMENT,
                    tf_directory,
                    variables,
                    description,
                    secrets["gitlab_token"],
                )

                # 3) No restore found =>  persist REQUESTED
                restore = BackupVaultRestore(
                    subscription_id=payload.subscription_id,
                    backup_vault_subscription_id=backup_vault["subscription_id"],
                    backup_vault_name=payload.backup_vault_name,
                    target_bucket_crn=target_bucket["bucket_crn"],
                    target_bucket_name=target_bucket["name"],
                    src_bucket_name=input_user_validation["source_bucket"]["name"],
                    recovery_range_id=recovery_range["recovery_range_id"],
                    restore_point_in_time=restore_point_in_time,
                    restore_type=restore_payload.get("restore_type", "in_place"),
                    status=RestoreStatus.REQUESTED.value,
                    action="restore",
                    created_by=payload.requestor,
                )

                db_restore = create_restore(session, restore)  # without restore id / status = requested

                logger.info("db_restore saved  %s", json.dumps(db_restore.to_dict(), default=str))

                # 4) No restore found => appel IBM
                state_manager.push_state({"workspace_name": ws_name})
                tf_workspace = tf.workspaces.get_by_id(create_ws_result["id"])
                tf_workspace.plan()
                apply_activity = tf_workspace.apply()
                # --- récupérer le restore_id via les outputs, puis persister ---
                ws_outputs = tf_workspace.get_outputs()  # adapte au nom exact dans la lib
                restore_id = None
                for out in ws_outputs:  # un par template_data
                    for values in out.output_values:  # liste de dicts d'outputs
                        if "restore_id" in values:
                            restore_id = values["restore_id"]["value"]
                            break
                    if restore_id:
                        break

                if restore_id is None:
                    raise ValueError("restore_id absent des outputs du workspace")

                logger.info("restore_id=%s", restore_id)

                mark_complete(session, restore.id, restore_id)
                return create_ws_result["id"]

            else:
                logger.info("Workspace already created")
                return workspace['workspace_id']

        except Exception as e:
            # update_bucket_status(payload.subscription_id, SubscriptionStatus.LOCKED, session)
            #   update_bucket_workspace_status(payload.subscription_id, Status.FAILED, session)
            if restore_id is not None:
                mark_failed(session, restore.id, restore_id, e)
            raise e
        finally:
            try:
                workspace = tf.workspaces.get_by_id(workspace_id=workspace['workspace_id'])
                workspace.delete()
            except Exception:
                logger.warning("ws cleanup failed ")

    input_user_validation = input_user_validation()
    iam_token = get_wklapp_iam_token()
    backup_vault = validate_backup_vault()
    restore_target = select_recovery_range(iam_token, input_user_validation)
    restore_result = create_tf_workspace_and_launch_restore(iam_token, input_user_validation, restore_target)


bucket_restore_backup_vault()
