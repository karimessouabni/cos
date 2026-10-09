"""DAG cos.bucket.v1.quarantine_test : éprouve la quarantaine seule, comme le clean s'en sert.

Pose la règle CBR du bucket par son workspace séparé (``quarantine_service``),
attend que le listing réponde 403, interroge l'API de configuration COS pendant
le blocage, ouvre la quarantaine (règle désactivée) et attend le 200, la
referme et attend le 403, la lève et attend le 200, puis met le compte rendu
dans le state. Ne touche ni au contenu du bucket ni à son statut de clean.

Le compte rendu donne les délais de propagation de chaque transition, qui
fixent la durée des ouvertures du clean (ADR 0004), et dit si l'API de
configuration passe la règle : si oui, le clean pourrait contrôler le vidage
sans ouvrir le bucket.

En mode ``report`` (``cos_quarantine_enforcement_mode``), rien n'est bloqué :
le compte rendu le dit, et les événements CBR du compte montrent ce qui
l'aurait été.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

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
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.clean_status import CleanStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

HTTP_FORBIDDEN = 403
HTTP_OK = 200
# Endpoint de l'API de configuration COS vu d'Airflow (Airflow Variable, sinon
# variable d'environnement) ; défaut : endpoint direct d'IBM.
CONFIG_API_SETTING = "cos_config_api_endpoint"


class BucketQuarantineTestPayload(ProductActionPayload):
    pass


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=BucketQuarantineTestPayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def bucket_quarantine_test() -> None:
    @step
    def validate_bucket(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketQuarantineTestPayload = depends(payload_dependency),
    ) -> dict:
        from cos_service.services.bucketService import get_bucket_by_sub_id

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        if bucket is None or not bucket["name"]:
            raise DeclineDemandException(
                f"the bucket doesn't exist or not fully created for the sub id {payload.subscription_id}"
            )
        if not bucket.get("virtual_server_endpoint"):
            raise DeclineDemandException(f"the bucket {bucket['name']} has no endpoint, its access cannot be probed")
        if bucket.get("clean_status") in [s.value for s in CleanStatus.busy()]:
            raise DeclineDemandException(
                f"a clean of the bucket {bucket['name']} is {bucket['clean_status']}: it already owns the quarantine"
            )
        if bucket.get("clean_cbr_workspace_id"):
            raise DeclineDemandException(
                f"the bucket {bucket['name']} still has a quarantine workspace ({bucket['clean_cbr_workspace_id']}): "
                "cancel_clean lifts it"
            )
        return dict(bucket)

    @step
    def get_cos_api_key(
        bucket: dict,
        reader: ReaderConnector = depends(reader_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> str:
        from cos_service.services.vault_service import get_cos_api_key

        return get_cos_api_key(bucket, vault, reader)

    @step
    def check_access_before(bucket: dict, api_key: str) -> int:
        """Le bucket doit répondre 200 avant : sinon le 403 attendu ne prouverait rien."""
        from cos_service.services.bucketService import bucket_access_status
        from cos_service.services.ibm_iam_service import get_iam_access_token

        status = bucket_access_status(get_iam_access_token(api_key), bucket)
        if status != HTTP_OK:
            raise DeclineDemandException(
                f"the bucket {bucket['name']} answers HTTP {status} before any quarantine: the test cannot conclude"
            )
        return status

    @step
    def set_quarantine(
        bucket: dict,
        access_before: int,
        payload: BucketQuarantineTestPayload = depends(payload_dependency),
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> str:
        """Pose la règle ; le workspace est noté en base pour que delete ou
        cancel_clean puissent le retirer si ce DAG s'arrête en route."""
        from cos_service.services.bucketService import set_bucket_clean_workspace
        from cos_service.services.quarantine_service import quarantine_settings, set_bucket_quarantine

        workspace_id = set_bucket_quarantine(tf=tf, bucket=bucket, payload=payload, vault=vault, reader=reader)
        set_bucket_clean_workspace(bucket["subscription_id"], workspace_id, session)
        state_manager.push_state({
            "quarantine_test": {
                "workspace_id": workspace_id,
                "enforcement_mode": quarantine_settings()["enforcement_mode"],
            }
        })
        return workspace_id

    @step
    def wait_until_blocked(bucket: dict, api_key: str, workspace_id: str) -> dict:
        """Sonde jusqu'au 403. Ne lève pas : la levée doit avoir lieu quoi qu'il arrive."""
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.quarantine_service import probe_bucket_until

        result = probe_bucket_until(get_iam_access_token(api_key), bucket, HTTP_FORBIDDEN)
        logger.info("quarantine of %s %s after %s s (HTTP %s)", bucket["name"],
                    "effective" if result["reached"] else "NOT effective", result["seconds"], result["status"])
        return result

    @step
    def check_config_api(bucket: dict, api_key: str, blocked: dict) -> dict:
        """Pendant le blocage, l'API de configuration COS répond-elle ? Si oui, le
        clean pourrait contrôler le vidage (``object_count``) bucket fermé.
        Informatif : ne lève jamais."""
        from cos_service.services.bucketService import COS_CONFIG_API, bucket_config_metadata
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.schematics_service import setting

        endpoint = setting(CONFIG_API_SETTING, COS_CONFIG_API)
        try:
            result = bucket_config_metadata(get_iam_access_token(api_key), bucket, endpoint)
        except Exception as exc:
            logger.exception("configuration API of %s unreachable at %s", bucket["name"], endpoint)
            return {"endpoint": endpoint, "status": None, "error": str(exc)}
        logger.info("configuration API of %s during the quarantine: HTTP %s", bucket["name"], result["status"])
        return {"endpoint": endpoint, **result}

    @step
    def reopen_quarantine(
        bucket: dict,
        api_key: str,
        workspace_id: str,
        config_api: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
    ) -> dict:
        """Ouverture comme au clean : règle désactivée, attente du 200. Ne lève pas."""
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.quarantine_service import open_bucket_quarantine

        try:
            return open_bucket_quarantine(
                tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader,
                access_token=get_iam_access_token(api_key),
            )
        except Exception as exc:
            logger.exception("opening the quarantine of %s failed", bucket["name"])
            return {"reached": False, "status": None, "error": str(exc)}

    @step
    def reclose_quarantine(
        bucket: dict,
        api_key: str,
        workspace_id: str,
        reopened: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
    ) -> dict:
        """Refermeture comme au clean : règle réactivée, attente du 403. Ne lève pas."""
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.quarantine_service import close_bucket_quarantine, probe_bucket_until

        try:
            close_bucket_quarantine(tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader)
        except Exception as exc:
            logger.exception("closing the quarantine of %s failed", bucket["name"])
            return {"reached": False, "status": None, "error": str(exc)}
        return probe_bucket_until(get_iam_access_token(api_key), bucket, HTTP_FORBIDDEN)

    @step
    def lift_quarantine(
        bucket: dict,
        reclosed: dict,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import get_bucket_by_sub_id, set_bucket_clean_workspace
        from cos_service.services.quarantine_service import lift_bucket_quarantine

        current = get_bucket_by_sub_id(session, bucket["subscription_id"]) or {}
        workspace_id = current.get("clean_cbr_workspace_id")
        if workspace_id:
            lift_bucket_quarantine(tf=tf, workspace_id=workspace_id)
            set_bucket_clean_workspace(bucket["subscription_id"], None, session)
        return True

    @step
    def wait_until_restored(
        bucket: dict,
        api_key: str,
        lifted: bool,
        blocked: dict,
        config_api: dict,
        reopened: dict,
        reclosed: dict,
        state_manager: StateManager = depends(state_manager_dependency),
    ) -> dict:
        """Sonde jusqu'au retour du 200, puis écrit le compte rendu complet dans le state.
        Verdict ok : bloqué, ouvert, refermé, puis rendu. L'API de configuration
        est informative et n'entre pas dans le verdict."""
        from cos_service.services.ibm_iam_service import get_iam_access_token
        from cos_service.services.quarantine_service import probe_bucket_until

        restored = probe_bucket_until(get_iam_access_token(api_key), bucket, HTTP_OK)
        steps = (blocked, reopened, reclosed, restored)
        report = {"blocked": blocked, "config_api": config_api, "reopened": reopened, "reclosed": reclosed,
                  "restored": restored, "verdict": "ok" if all(r.get("reached") for r in steps) else "ko"}
        state_manager.push_state({"quarantine_test": report})
        logger.info("quarantine test of %s: %s", bucket["name"], report["verdict"])
        if not restored["reached"]:
            raise RuntimeError(
                f"the bucket {bucket['name']} still answers HTTP {restored['status']} after the quarantine was lifted"
            )
        return report

    bucket = validate_bucket()
    api_key = get_cos_api_key(bucket=bucket)
    access_before = check_access_before(bucket=bucket, api_key=api_key)
    workspace_id = set_quarantine(bucket=bucket, access_before=access_before)
    blocked = wait_until_blocked(bucket=bucket, api_key=api_key, workspace_id=workspace_id)
    config_api = check_config_api(bucket=bucket, api_key=api_key, blocked=blocked)
    reopened = reopen_quarantine(bucket=bucket, api_key=api_key, workspace_id=workspace_id, config_api=config_api)
    reclosed = reclose_quarantine(bucket=bucket, api_key=api_key, workspace_id=workspace_id, reopened=reopened)
    lifted = lift_quarantine(bucket=bucket, reclosed=reclosed)
    wait_until_restored(bucket=bucket, api_key=api_key, lifted=lifted, blocked=blocked, config_api=config_api,
                        reopened=reopened, reclosed=reclosed)


bucket_quarantine_test()
