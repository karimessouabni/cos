"""DAG cos.bucket.v1.cancel_clean : annule un clean pendant sa période de grâce (v1).

Accepté sur un clean ``scheduled`` dont la date d'exécution n'est pas passée,
et sur un clean ``failed`` (le remettre dans un état d'où un nouveau clean est
possible). Le passage à ``cancelled`` est une mise à jour conditionnelle : si
le clean a commencé entre-temps, l'annulation est refusée, il n'y a plus de
retour en arrière une fois la règle d'expiration posée.
Voir docs/adr/0003-periode-de-grace-du-clean.md.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

import logging  # noqa: E402 - après add_project_to_path()
from datetime import datetime, timezone  # noqa: E402 - après add_project_to_path()
from pathlib import Path  # noqa: E402 - après add_project_to_path()

from bp2i_airflow_library.config import ENVIRONMENT  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dag import product_action, step  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.dependencies import (  # noqa: E402 - après add_project_to_path()
    SASession,
    StateManager,
    depends,
    payload_dependency,
    sqlalchemy_session_dependency,
    state_manager_dependency,
)
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException  # noqa: E402 - après add_project_to_path()
from bp2i_airflow_library.schemas import ProductActionConfig, ProductActionPayload  # noqa: E402 - après add_project_to_path()

from cos_service.schemas.clean_status import CleanStatus  # noqa: E402 - après add_project_to_path()

logger = logging.getLogger(__name__)

CANCELLABLE = (CleanStatus.SCHEDULED, CleanStatus.FAILED)


class BucketCancelCleanPayload(ProductActionPayload):
    pass


def _as_datetime(value) -> datetime | None:
    """``clean_execute_at`` tel que la base le rend (datetime, ou ISO 8601) ; UTC si naïf."""
    if value is None:
        return None
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


@product_action(
    action_id=Path(__file__).stem.replace(".v1.", ".v2.")
    if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
    else Path(__file__).stem,
    tags=["cos"],
    payload=BucketCancelCleanPayload,
    config=ProductActionConfig(lock_subscription_on_failure=False),
)
def bucket_cancel_clean() -> None:
    @step
    def validate_bucket(
        session: SASession = depends(sqlalchemy_session_dependency),
        payload: BucketCancelCleanPayload = depends(payload_dependency),
    ) -> dict:
        from cos_service.services.bucketService import get_bucket_by_sub_id

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)
        if bucket is None or not bucket["name"]:
            raise DeclineDemandException(
                f"the bucket doesn't exist or not fully created for the sub id {payload.subscription_id}"
            )
        status = bucket.get("clean_status")
        if status not in [s.value for s in CANCELLABLE]:
            raise DeclineDemandException(
                f"no clean to cancel on the bucket {bucket['name']} (clean status: {status}); "
                "a clean can be cancelled while scheduled or after a failure"
            )
        execute_at = _as_datetime(bucket.get("clean_execute_at"))
        if status == CleanStatus.SCHEDULED.value and execute_at is not None \
                and datetime.now(timezone.utc) >= execute_at:
            raise DeclineDemandException(
                f"the grace period of the clean of {bucket['name']} ended at {execute_at.isoformat()}: "
                "the deletion is starting, it cannot be cancelled any more"
            )
        return dict(bucket)

    @step
    def cancel_clean(
        bucket: dict,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """``scheduled | failed -> cancelled`` en une mise à jour conditionnelle :
        si le clean vient de démarrer, l'annulation est refusée."""
        from cos_service.services.bucketService import transition_bucket_clean

        if not transition_bucket_clean(
            bucket["subscription_id"], CleanStatus(bucket["clean_status"]), CleanStatus.CANCELLED, session
        ):
            raise DeclineDemandException(
                f"the clean of the bucket {bucket['name']} just started, it cannot be cancelled any more"
            )
        state_manager.push_state({"clean_status": CleanStatus.CANCELLED.value})
        logger.info("clean of bucket %s cancelled", bucket["name"])
        return True

    bucket = validate_bucket()
    cancel_clean(bucket=bucket)


bucket_cancel_clean()
