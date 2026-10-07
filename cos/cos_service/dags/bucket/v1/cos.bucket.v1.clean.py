"""DAG cos.bucket.v1.clean : vide un bucket par une règle d'expiration, après une
période de grâce annulable pendant laquelle il est en quarantaine (v2).

Déroulé (docs/adr/0003-periode-de-grace-du-clean.md) :

1. validation en base, puis contrôle des verrous par le listing : un objet
   protégé par la politique du bucket (rétention IBM, Object Lock) est libre
   au plus tard à ``LastModified + durée maximale`` ; si cette borne dépasse
   la fin de la grâce, la demande est refusée avec la date à laquelle relancer ;
2. ``clean_status = scheduled`` avec la date d'exécution, visible dans le state ;
3. quarantaine : règle CBR posée par un workspace Schematics séparé du bucket
   (``quarantine_service``) ; les clients, sur l'URL publique, reçoivent 403,
   l'orchestrateur garde l'accès ;
4. attente différée jusqu'à la date (``DateTimeSensorAsync``, aucun worker occupé) ;
5. décision atomique en base : ``scheduled -> in_progress``. Si le clean a été
   annulé entre-temps (``cos.bucket.v1.cancel_clean``, qui lève la quarantaine),
   la demande est déclinée et rien n'est supprimé ;
6. vidage : règle d'expiration à 1 jour, sensor jusqu'au vide, retrait de la règle ;
7. levée de la quarantaine, ``clean_status = success``.

Aucune configuration destructrice n'est posée avant l'étape 6. Un échec après
la décision laisse le bucket en quarantaine et ``failed`` : ``cancel_clean``
lève la quarantaine.

La clé API COS est lue deux fois (contrôle des verrous, puis vidage) : son
bail Vault peut être plus court que la grâce, elle ne traverse pas l'attente.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

import logging  # noqa: E402 - après add_project_to_path()
from datetime import datetime, timedelta, timezone  # noqa: E402 - après add_project_to_path()
from pathlib import Path  # noqa: E402 - après add_project_to_path()

from airflow.sensors.base import PokeReturnValue  # noqa: E402 - après add_project_to_path()
from airflow.sensors.date_time import DateTimeSensorAsync  # noqa: E402 - après add_project_to_path()
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
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload  # noqa: E402 - après add_project_to_path()
from dateutil.relativedelta import relativedelta  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.clean_status import CleanStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

# Période de grâce avant toute suppression, en minutes : 7 jours par défaut,
# réglable (Airflow Variable cos_clean_grace_minutes, sinon
# $COS_CLEAN_GRACE_MINUTES) pour les tests toolchain en INT, qui ne peuvent
# pas attendre sept jours.
GRACE_PERIOD_SETTING = "cos_clean_grace_minutes"
DEFAULT_GRACE_PERIOD = timedelta(days=7)

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


def grace_period() -> timedelta:
    from cos_service.services.schematics_service import setting

    return timedelta(minutes=int(setting(GRACE_PERIOD_SETTING, str(int(DEFAULT_GRACE_PERIOD.total_seconds() // 60)))))


def _mark_failed(subscription_id: str, state_manager: StateManager, session: SASession) -> None:
    from cos_service.services.bucketService import update_bucket_clean_status

    update_bucket_clean_status(subscription_id, CleanStatus.FAILED, session)
    state_manager.push_state({"clean_status": CleanStatus.FAILED.value})


def _is_not_found(exc: Exception) -> bool:
    """Une erreur S3 porte ``code`` ; toute autre exception n'est pas un 404."""
    return str(getattr(exc, "code", None)) == S3_NOT_FOUND


def lock_policy(bucket: dict) -> tuple[str, relativedelta] | None:
    """Politique de verrouillage du bucket et sa durée maximale, ou None.

    Rétention IBM : ``retention_maximum`` en jours (bucket non versionné).
    Object Lock : ``object_lock_duration_days`` ou ``_years`` (bucket versionné,
    chaque version porte son verrou). Les deux s'excluent (documentation IBM).
    """
    if bucket.get("retention_enabled"):
        days = bucket.get("retention_maximum") or bucket.get("retention_default")
        return "retention", relativedelta(days=int(days)) if days else None
    if bucket.get("object_lock_duration_days"):
        return "object lock", relativedelta(days=int(bucket["object_lock_duration_days"]))
    if bucket.get("object_lock_duration_years"):
        return "object lock", relativedelta(years=int(bucket["object_lock_duration_years"]))
    return None


def locked_until(bucket: dict, latest_modification: datetime | None) -> datetime | None:
    """Borne de fin des verrous : ``LastModified`` le plus récent + durée maximale.
    None sans politique, ou bucket vide."""
    policy = lock_policy(bucket)
    if policy is None or latest_modification is None:
        return None
    _, duration = policy
    if duration is None:
        return None
    return latest_modification + duration


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
        payload: BucketCleanPayload = depends(payload_dependency),
    ) -> dict:
        """Refuse ce qui ne peut pas être nettoyé d'après la base. Ne change rien."""
        from cos_service.services.bucketService import get_bucket_by_sub_id

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)

        if bucket is None or not bucket["name"]:
            raise DeclineDemandException(
                f"the bucket doesn't exist or not fully created for the sub id {payload.subscription_id}"
            )
        if bucket["clean_status"] in [s.value for s in CleanStatus.busy()]:
            raise DeclineDemandException(
                f"a clean of the bucket {bucket['name']} is already {bucket['clean_status']}"
                + (f" (execution planned at {bucket['clean_execute_at']}, cancel it first)"
                   if bucket.get("clean_execute_at") else "")
            )
        if not bucket.get("virtual_server_endpoint"):
            raise DeclineDemandException(f"the bucket {bucket['name']} has no endpoint, it cannot be listed")

        return dict(bucket)

    @step
    def check_locks(
        bucket: dict,
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
    ) -> str | None:
        """Date de fin des verrous (ISO 8601) ou None. Refuse si elle dépasse la fin
        de la grâce : les objets protégés ne pourraient pas expirer à temps."""
        from cos_service.services.bucketService import latest_object_modification
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.vault_service import get_cos_api_key

        policy = lock_policy(bucket)
        if policy is None:
            return None
        kind, duration = policy
        if duration is None:
            raise DeclineDemandException(
                f"the bucket {bucket['name']} has a {kind} without a known maximum duration: "
                "the end of the locks cannot be computed, it cannot be cleaned"
            )
        access_token = get_iam_access_token(get_cos_api_key(bucket, vault, reader))
        latest = latest_object_modification(access_token, bucket, versions=(kind == "object lock"))
        until = locked_until(bucket, latest)
        if until is None:
            logger.info("%s has a %s but no object: nothing is locked", bucket["name"], kind)
            return None
        execute_at = datetime.now(timezone.utc) + grace_period()
        if until > execute_at:
            raise DeclineDemandException(
                f"the bucket {bucket['name']} has a {kind}: its objects may stay locked until "
                f"{until.isoformat()}, after the end of the grace period ({execute_at.isoformat()}). "
                "Locked objects cannot be deleted; request the clean again after that date, "
                "or delete the unlocked objects yourself."
            )
        logger.info("%s: last %s expires at %s, before the end of the grace period", bucket["name"], kind, until.isoformat())
        return until.isoformat()

    @step
    def schedule_clean(
        bucket: dict,
        locked_until: str | None,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> str:
        """``clean_status = scheduled`` avec la date d'exécution ; la renvoie (ISO 8601, UTC)
        pour le sensor. Le client la voit dans le state."""
        from cos_service.services.bucketService import schedule_bucket_clean

        requested_at = datetime.now(timezone.utc)
        execute_at = requested_at + grace_period()
        schedule_bucket_clean(bucket["subscription_id"], requested_at, execute_at, session)
        state_manager.push_state({
            "clean_status": CleanStatus.SCHEDULED.value,
            "clean_requested_at": requested_at.isoformat(),
            "clean_execute_at": execute_at.isoformat(),
            "clean_notice": (
                f"Every object of the bucket, including those written before {execute_at.isoformat()}, "
                "will be deleted at that date. Cancel the clean before then if needed."
            ),
        })
        logger.info("clean of bucket %s scheduled at %s", bucket["name"], execute_at.isoformat())
        return execute_at.isoformat()

    @step
    def quarantine_bucket(
        bucket: dict,
        execute_at: str,
        payload: BucketCleanPayload = depends(payload_dependency),
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> str:
        """Pose la règle CBR par le workspace de quarantaine du bucket et garde
        l'identifiant du workspace en base, pour la levée. ``execute_at`` ordonne
        l'étape après la programmation."""
        from cos_service.services.bucketService import set_bucket_clean_workspace
        from cos_service.services.quarantine_service import set_bucket_quarantine

        try:
            workspace_id = set_bucket_quarantine(tf=tf, bucket=bucket, payload=payload, vault=vault, reader=reader)
            set_bucket_clean_workspace(bucket["subscription_id"], workspace_id, session)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        state_manager.push_state({"quarantine": True})
        return workspace_id

    @step
    def claim_clean(
        bucket: dict,
        quarantine_workspace_id: str,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Fin de la grâce : ``scheduled -> in_progress`` en une mise à jour
        conditionnelle. Si elle ne passe pas, le clean a été annulé entre-temps :
        la demande est déclinée, rien n'est supprimé (l'annulation a levé la
        quarantaine)."""
        from cos_service.services.bucketService import get_bucket_by_sub_id, transition_bucket_clean

        if not transition_bucket_clean(
            bucket["subscription_id"], CleanStatus.SCHEDULED, CleanStatus.INPROGRESS, session
        ):
            current = get_bucket_by_sub_id(session, bucket["subscription_id"]) or {}
            raise DeclineDemandException(
                f"the clean of the bucket {bucket['name']} was cancelled during the grace period "
                f"(status: {current.get('clean_status')}), nothing was deleted"
            )
        state_manager.push_state({"clean_status": CleanStatus.INPROGRESS.value})
        return True

    @step
    def get_cos_api_key(
        bucket: dict,
        claimed: bool,
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
        """Trace la règle en base et renvoie ``is_expiration_created`` tel quel.

        Le sensor consomme ce résultat (et non le drapeau d'origine) : c'est ce
        qui fait de la sauvegarde une étape amont, Airflow ne déduisant l'ordre
        que des valeurs consommées. Un mot-clé supplémentaire sur un
        ``step.sensor`` est refusé par la bibliothèque ("unexpected keyword"),
        d'où le passage par le drapeau existant."""
        from cos_service.services.lifecyclePolicyRuleService import (
            complete_lifecycle_policy_rule_creation,
            disable_lifecycle_policy_rules_by_bucket_sub_id,
        )

        if not is_expiration_created:
            return False
        try:
            disable_lifecycle_policy_rules_by_bucket_sub_id(bucket, payload.requestor, session)
            complete_lifecycle_policy_rule_creation(
                bucket, CLEAN_RULE_ID, CLEAN_RULE_PREFIX, CLEAN_EXPIRATION_DAYS, CLEAN_EXPIRATION_DAYS,
                payload.requestor, session,
            )
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        return is_expiration_created

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
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> PokeReturnValue:
        """Sonde le bucket jusqu'à ce qu'il soit vide ; sans règle posée, terminé tout de suite.
        ``is_expiration_created`` vient de la sauvegarde en base : le sensor l'attend."""
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
        """Retire la règle une fois le bucket vide, seulement si elle a été posée :
        un bucket déjà vide garde sa configuration de cycle de vie. ``check_clean_done``
        (résultat du sensor) est toujours vrai et ne sert qu'à ordonner les étapes.
        Vrai si la règle est partie (ou avait déjà disparu : 404)."""
        from cos_service.services.bucketService import delete_lifecycle_policy
        from cos_service.services.ibm_iam_service import get_iam_access_token

        if not is_expiration_created:
            return False
        try:
            access_token = get_iam_access_token(api_key)
            delete_lifecycle_policy(access_token, bucket)
        except Exception as exc:
            if not _is_not_found(exc):
                _mark_failed(bucket["subscription_id"], state_manager, session)
                raise
            logger.info("lifecycle policy of %s already gone (404)", bucket["name"])
        return True

    @step
    def save_delete_expiration_rule_in_db(
        is_expiration_deleted: bool,
        bucket: dict,
        payload: BucketCleanPayload = depends(payload_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.lifecyclePolicyRuleService import disable_lifecycle_policy_rules_by_bucket_sub_id

        if not is_expiration_deleted:
            return True
        try:
            disable_lifecycle_policy_rules_by_bucket_sub_id(bucket, payload.requestor, session)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        return True

    @step
    def lift_quarantine(
        bucket: dict,
        rules_saved: bool,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Lève la quarantaine (workspace relu en base : il a été créé après la
        validation), puis ``clean_status = success``. En cas d'échec le bucket
        reste en quarantaine et ``failed`` : ``cancel_clean`` la lève."""
        from cos_service.services.bucketService import (
            get_bucket_by_sub_id,
            set_bucket_clean_workspace,
            update_bucket_clean_status,
        )
        from cos_service.services.quarantine_service import lift_bucket_quarantine

        current = get_bucket_by_sub_id(session, bucket["subscription_id"]) or {}
        workspace_id = current.get("clean_cbr_workspace_id")
        try:
            if workspace_id:
                lift_bucket_quarantine(tf=tf, workspace_id=workspace_id)
                set_bucket_clean_workspace(bucket["subscription_id"], None, session)
        except Exception:
            _mark_failed(bucket["subscription_id"], state_manager, session)
            raise
        update_bucket_clean_status(bucket["subscription_id"], CleanStatus.SUCCESS, session)
        state_manager.push_state({"clean_status": CleanStatus.SUCCESS.value, "quarantine": False})
        return True

    bucket = validate_bucket()
    lock_check = check_locks(bucket=bucket)
    execute_at = schedule_clean(bucket=bucket, locked_until=lock_check)
    quarantine_workspace_id = quarantine_bucket(bucket=bucket, execute_at=execute_at)

    wait = DateTimeSensorAsync(
        task_id="wait_for_grace_period",
        target_time="{{ ti.xcom_pull(task_ids='%s') }}" % execute_at.operator.task_id,
    )
    quarantine_workspace_id >> wait

    claimed = claim_clean(bucket=bucket, quarantine_workspace_id=quarantine_workspace_id)
    wait >> claimed

    api_key = get_cos_api_key(bucket=bucket, claimed=claimed)
    is_bucket_empty = is_bucket_empty(bucket=bucket, api_key=api_key)
    is_expiration_created = create_expiration_rule(api_key=api_key, bucket=bucket, is_bucket_empty=is_bucket_empty)
    # Le sensor consomme le drapeau renvoyé par la sauvegarde en base, pas celui
    # de create_expiration_rule : il attend ainsi que la règle soit tracée.
    rule_saved = save_create_expiration_rule_in_db(is_expiration_created=is_expiration_created, bucket=bucket)
    check_clean_done = scheduler_clean_bucket(bucket=bucket, api_key=api_key, is_expiration_created=rule_saved)
    is_expiration_deleted = delete_expiration_rule(
        bucket=bucket, api_key=api_key, is_expiration_created=is_expiration_created, check_clean_done=check_clean_done
    )
    rules_saved = save_delete_expiration_rule_in_db(is_expiration_deleted=is_expiration_deleted, bucket=bucket)
    lift_quarantine(bucket=bucket, rules_saved=rules_saved)


bucket_clean()
