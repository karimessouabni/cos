"""DAG cos.bucket.v1.quarantine_test : éprouve le mécanisme de quarantaine seul.

Pose la règle CBR du bucket par son workspace séparé (``quarantine_service``),
attend que le listing du bucket depuis Airflow réponde 403, fait lister le
bucket depuis Schematics (qui doit passer, c'est lui qui videra le bucket),
lève la quarantaine, attend que l'accès revienne, et met le compte rendu dans
le state. Ne touche ni au contenu du bucket ni à son statut de clean. À lancer
en INT avant la période de grâce, pour vérifier le rôle CBR de l'identité
Schematics, la référence de service du compte hub (``cos_hub_account_id``), le
workspace et le délai de propagation (quelques minutes).

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
import time  # noqa: E402 - après add_project_to_path()
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

# Propagation d'une règle CBR : quelques minutes. Sondage toutes les 30 s, 20 min au plus.
PROBE_INTERVAL_SECONDS = 30
PROBE_MAX_ATTEMPTS = 40
HTTP_FORBIDDEN = 403
HTTP_OK = 200


class BucketQuarantineTestPayload(ProductActionPayload):
    pass


def probe_until(access_token: str, bucket: dict, expected: int,
                sleep=None, interval: int | None = None, attempts: int | None = None) -> dict:
    """Liste le bucket jusqu'au statut attendu. Ne lève jamais : le compte
    rendu dit si l'attente a abouti, en combien de temps, et sur quel statut.
    Les défauts sont résolus à l'appel (les tests les remplacent sur le module)."""
    from cos_service.services.bucketService import bucket_access_status

    sleep = sleep or time.sleep
    interval = PROBE_INTERVAL_SECONDS if interval is None else interval
    attempts = PROBE_MAX_ATTEMPTS if attempts is None else attempts
    seen = []
    for attempt in range(1, attempts + 1):
        status = bucket_access_status(access_token, bucket)
        seen.append(status)
        if status == expected:
            return {"reached": True, "status": status, "attempts": attempt, "seconds": (attempt - 1) * interval}
        if attempt < attempts:
            sleep(interval)
    return {"reached": False, "status": seen[-1], "attempts": attempts, "seconds": (attempts - 1) * interval,
            "statuses_seen": sorted(set(seen))}


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

        result = probe_until(get_iam_access_token(api_key), bucket, HTTP_FORBIDDEN)
        logger.info("quarantine of %s %s after %s s (HTTP %s)", bucket["name"],
                    "effective" if result["reached"] else "NOT effective", result["seconds"], result["status"])
        return result

    @step
    def check_schematics_access(
        bucket: dict,
        blocked: dict,
        workspace_id: str,
        tf: SchematicsBackend = depends(smart_schematics_backend_dependency),
        vault: Vault = depends(vault_dependency),
        reader: ReaderConnector = depends(reader_dependency),
    ) -> dict:
        """L'autre face de la règle : Schematics doit lister le bucket (200) pendant
        qu'Airflow est bloqué. Ne lève pas : la levée doit avoir lieu quoi qu'il arrive."""
        from cos_service.services.quarantine_service import SCOPE_SCHEMATICS, probe_bucket_via_schematics

        try:
            probe = probe_bucket_via_schematics(tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader)
        except Exception as exc:  # un workspace en échec ne doit pas laisser le bucket en quarantaine
            logger.exception("probe of %s from Schematics failed", bucket["name"])
            return {"reached": False, "status": None, "empty": None, "scope": None, "error": str(exc)}
        expected = probe["scope"] == SCOPE_SCHEMATICS
        reached = probe["status"] == HTTP_OK
        logger.info("Schematics %s the bucket %s during the quarantine (HTTP %s, scope %s)",
                    "can list" if reached else "CANNOT list", bucket["name"], probe["status"], probe["scope"])
        return {"reached": reached, "expected": expected, **probe}

    @step
    def lift_quarantine(
        bucket: dict,
        schematics: dict,
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
        schematics: dict,
        state_manager: StateManager = depends(state_manager_dependency),
    ) -> dict:
        """Sonde jusqu'au retour du 200, puis écrit le compte rendu complet dans le state.
        Verdict ok : Airflow bloqué, Schematics passé si la zone le prévoit, accès rendu."""
        from cos_service.services.ibm_iam_service import get_iam_access_token

        restored = probe_until(get_iam_access_token(api_key), bucket, HTTP_OK)
        schematics_ok = schematics["reached"] or not schematics.get("expected")
        report = {"blocked": blocked, "schematics": schematics, "restored": restored,
                  "verdict": "ok" if blocked["reached"] and schematics_ok and restored["reached"] else "ko"}
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
    schematics = check_schematics_access(bucket=bucket, blocked=blocked, workspace_id=workspace_id)
    lifted = lift_quarantine(bucket=bucket, schematics=schematics)
    wait_until_restored(bucket=bucket, api_key=api_key, lifted=lifted, blocked=blocked, schematics=schematics)


bucket_quarantine_test()
