"""DAG cos.bucket.v1.clean : vide un bucket par une règle d'expiration, puis la retire.

Corrections par rapport au fichier d'entreprise : la règle n'est retirée que si
elle a été posée (un bucket déjà vide garde sa configuration de cycle de vie),
les buckets à rétention / object lock sont refusés (leurs objets ne peuvent pas
expirer), le sensor a un timeout explicite, le ``success`` est posé après le
retrait de la règle, un 404 au retrait compte comme retiré, toute exception
(pas seulement S3) passe le clean en ``failed``, ``in_progress`` n'est posé
qu'une fois, échec factorisé dans ``_mark_failed``, et le sensor attend la
sauvegarde de la règle en base (qui n'était reliée à rien).
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

import logging  # noqa: E402 - après add_project_to_path()
from pathlib import Path  # noqa: E402 - après add_project_to_path()

from airflow.sensors.base import PokeReturnValue  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.config import ENVIRONMENT  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.connectors.reader import ReaderConnector  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dag import product_action, step  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dependencies import (  # noqa: E402 - après add_project_to_path()
    SASession,
    StateManager,
    Vault,
    depends,
    payload_dependency,
    reader_dependency,
    sqlalchemy_session_dependency,
    state_manager_dependency,
    vault_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.status import Status  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

# Règle d'expiration posée pour vider le bucket : tout le bucket (préfixe vide),
# objets courants et versions non courantes expirés après 1 jour.
CLEAN_RULE_ID = "clean_bucket"
CLEAN_RULE_PREFIX = ""
CLEAN_EXPIRATION_DAYS = 1
# COS évalue les règles d'expiration une fois par jour : un sondage toutes les
# 3 h suffit (mode reschedule, le worker n'est pas occupé entre deux sondages).
# Le timeout borne l'attente : au-delà, le sensor échoue au lieu de laisser le
# clean "in_progress" sans fin.
CLEAN_POKE_INTERVAL_SECONDS = 3 * 3600
CLEAN_TIMEOUT_SECONDS = 7 * 24 * 3600
S3_NOT_FOUND = "404"


class BucketCleanPayload(ProductActionPayload):
    pass


def _mark_failed(subscription_id: str, state_manager: StateManager, session: SASession) -> None:
    from cos_service.services.bucketService import update_bucket_clean_status

    update_bucket_clean_status(subscription_id, Status.FAILED, session)
    state_manager.push_state({"clean_status": Status.FAILED.value})


def _is_not_found(exc: Exception) -> bool:
    """Une erreur S3 porte ``code`` ; toute autre exception n'est pas un 404."""
    return str(getattr(exc, "code", None)) == S3_NOT_FOUND


def _is_locked(bucket: dict) -> bool:
    """Rétention ou object lock : les objets verrouillés ne peuvent pas expirer."""
    return bool(
        bucket.get("retention_enabled")
        or bucket.get("object_lock_duration_days")
        or bucket.get("object_lock_duration_years")
    )


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=BucketCleanPayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def bucket_clean() -> None:
    @step
    def validate_bucket(
        session: SASession = depends(sqlalchemy_session_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        payload: BucketCleanPayload = depends(payload_dependency),
    ) -> dict:
        """Refuse ce qui ne peut pas être nettoyé, puis pose ``clean_status = in_progress``
        (une seule fois : les étapes suivantes ne le reposent pas)."""
        from cos_service.services.bucketService import (
            get_bucket_by_sub_id,
            update_bucket_clean_status,
        )

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)

        if bucket is None or not bucket["name"]:
            raise DeclineDemandException(
                f"the bucket doesn't exist or not fully created for the sub id {payload.subscription_id}"
            )
        if bucket["clean_status"] == Status.INPROGRESS.value:
            raise DeclineDemandException("the clean action is already in progress...")
        if _is_locked(bucket):
            raise DeclineDemandException(
                f"the bucket {bucket['name']} has a retention or object lock: "
                "locked objects cannot expire, it cannot be cleaned"
            )

        update_bucket_clean_status(payload.subscription_id, Status.INPROGRESS, session)
        state_manager.push_state({"clean_status": Status.INPROGRESS.value})

        return dict(bucket)

    @step
    def get_cos_api_key(
        bucket: dict,
        session: SASession = depends(sqlalchemy_session_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> str:
        from cos_service.services.vault_service import get_cos_api_key

        try:
            return get_cos_api_key(bucket, vault, reader)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise

    @step
    def is_bucket_empty(
        bucket: dict,
        api_key: str,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import check_bucket_has_contents
        from cos_service.services.ibm_iam_service import get_iam_access_token

        try:
            access_token = get_iam_access_token(api_key)
            has_contents = check_bucket_has_contents(access_token, bucket)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        if not has_contents:
            logger.info("%s is empty.", bucket["name"])
        return not has_contents

    @step
    def create_expiration_rule(
        api_key: str,
        bucket: dict,
        is_bucket_empty: bool,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Vrai si une règle d'expiration a été posée (bucket non vide)."""
        from cos_service.services.bucketService import create_expiration_rule
        from cos_service.services.ibm_iam_service import get_iam_access_token

        if is_bucket_empty:
            return False
        try:
            access_token = get_iam_access_token(api_key)
            return create_expiration_rule(access_token, bucket)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise

    @step
    def save_create_expiration_rule_in_db(
        is_expiration_created: bool,
        bucket: dict,
        payload: BucketCleanPayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Trace la règle en base. Renvoie vrai : le sensor consomme ce résultat,
        c'est ce qui en fait une étape amont (Airflow ne déduit l'ordre que des
        valeurs consommées ; sans ça la sauvegarde échouait dans son coin et le
        clean continuait)."""
        from cos_service.services.lifecyclePolicyRuleService import (
            complete_lifecycle_policy_rule_creation,
            disable_lifecycle_policy_rules_by_bucket_sub_id,
        )

        if not is_expiration_created:
            return True
        try:
            disable_lifecycle_policy_rules_by_bucket_sub_id(bucket, payload.requestor, session)
            complete_lifecycle_policy_rule_creation(
                bucket, CLEAN_RULE_ID, CLEAN_RULE_PREFIX, CLEAN_EXPIRATION_DAYS, CLEAN_EXPIRATION_DAYS,
                payload.requestor, session,
            )
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        return True

    @step.sensor(
        exponential_backoff=False,
        poke_interval=CLEAN_POKE_INTERVAL_SECONDS,
        timeout=CLEAN_TIMEOUT_SECONDS,
        mode="reschedule",
    )
    def scheduler_clean_bucket(
        bucket: dict,
        api_key: str,
        is_expiration_created: bool,
        rule_saved: bool,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> PokeReturnValue:
        """Sonde le bucket jusqu'à ce qu'il soit vide ; sans règle posée, terminé tout de suite.
        ``rule_saved`` n'est là que pour attendre la sauvegarde en base."""
        from cos_service.services.bucketService import check_bucket_has_contents
        from cos_service.services.ibm_iam_service import get_iam_access_token

        if not is_expiration_created:
            return PokeReturnValue(is_done=True, xcom_value={"content": "clean"})
        try:
            access_token = get_iam_access_token(api_key)
            has_contents = check_bucket_has_contents(access_token, bucket)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        return PokeReturnValue(is_done=not has_contents, xcom_value={"content": "clean"})

    @step
    def delete_expiration_rule(
        bucket: dict,
        api_key: str,
        is_expiration_created: bool,
        check_clean_done: PokeReturnValue,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Retire la règle une fois le bucket vide et pose ``clean_status = success``.

        Seulement si une règle a été posée : ``check_clean_done`` (résultat du
        sensor) est toujours vrai et ne sert qu'à ordonner les étapes. Un bucket
        déjà vide garde ainsi sa configuration de cycle de vie. Vrai si la règle
        est partie (ou avait déjà disparu : 404).
        """
        from cos_service.services.bucketService import delete_lifecycle_policy, update_bucket_clean_status
        from cos_service.services.ibm_iam_service import get_iam_access_token

        if is_expiration_created:
            try:
                access_token = get_iam_access_token(api_key)
                delete_lifecycle_policy(access_token, bucket)
            except Exception as exc:
                if not _is_not_found(exc):
                    _mark_failed(bucket["subscription_id"], state_manager, session)
                    raise
                logger.info("lifecycle policy of %s already gone (404)", bucket["name"])
        update_bucket_clean_status(bucket["subscription_id"], Status.SUCCESS, session)
        state_manager.push_state({"clean_status": Status.SUCCESS.value})
        return is_expiration_created

    @step
    def save_delete_expiration_rule_in_db(
        is_expiration_deleted: bool,
        bucket: dict,
        payload: BucketCleanPayload = depends(payload_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> None:
        from cos_service.services.lifecyclePolicyRuleService import disable_lifecycle_policy_rules_by_bucket_sub_id

        if is_expiration_deleted:
            disable_lifecycle_policy_rules_by_bucket_sub_id(bucket, payload.requestor, session)

    bucket = validate_bucket()
    api_key = get_cos_api_key(bucket=bucket)
    is_bucket_empty = is_bucket_empty(bucket=bucket, api_key=api_key)
    is_expiration_created = create_expiration_rule(api_key=api_key, bucket=bucket, is_bucket_empty=is_bucket_empty)
    rule_saved = save_create_expiration_rule_in_db(is_expiration_created=is_expiration_created, bucket=bucket)
    check_clean_done = scheduler_clean_bucket(
        bucket=bucket, api_key=api_key, is_expiration_created=is_expiration_created, rule_saved=rule_saved
    )
    is_expiration_deleted = delete_expiration_rule(
        bucket=bucket, api_key=api_key, is_expiration_created=is_expiration_created, check_clean_done=check_clean_done
    )
    save_delete_expiration_rule_in_db(is_expiration_deleted=is_expiration_deleted, bucket=bucket)


bucket_clean()
