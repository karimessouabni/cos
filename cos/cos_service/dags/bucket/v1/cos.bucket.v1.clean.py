"""DAG cos.bucket.v1.clean : vide un bucket par une règle d'expiration, puis la retire.

[PARTIEL] Lignes 21 à 138 du fichier d'entreprise reprises telles quelles
(captures). L'en-tête (lignes 1 à 20) suit le modèle des autres DAGs ; le
corps de ``create_expiration_rule``, les étapes suivantes (dont le sensor qui
importe ``PokeReturnValue``) et l'enchaînement final restent à reporter.
"""
from bp2i_airflow_library import add_project_to_path

add_project_to_path()

try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = False

import logging  # noqa: E402 - après add_project_to_path()
from pathlib import Path  # noqa: E402 - après add_project_to_path()

from airflow.sensors.base import PokeReturnValue  # noqa: E402, F401 - après add_project_to_path()
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

TERRAFORM_REPOSITORY = "https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/products/cos/cos.git"

logger = logging.getLogger(__name__)


class BucketCleanPayload(ProductActionPayload):
    pass


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
        from cos_service.services.bucketService import (
            get_bucket_by_sub_id,
            update_bucket_clean_status,
        )

        bucket = get_bucket_by_sub_id(session, payload.subscription_id)

        if bucket is None or not bucket["name"]:
            raise DeclineDemandException(
                f"the bucket doesn't exist or not fully created for the sub id ,{payload.subscription_id}"
            )
        if bucket["clean_status"] == Status.INPROGRESS.value:
            raise DeclineDemandException("the clean action is already in progress...")

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
        from cos_service.services.bucketService import update_bucket_clean_status
        from cos_service.services.vault_service import get_cos_api_key

        try:
            update_bucket_clean_status(bucket["subscription_id"], Status.INPROGRESS, session)
            return get_cos_api_key(bucket, vault, reader)
        except Exception as e:
            update_bucket_clean_status(bucket["subscription_id"], Status.FAILED, session)
            state_manager.push_state({"clean_status": Status.FAILED.value})
            raise e

    @step
    def is_bucket_empty(
        bucket: dict,
        api_key: str,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        from cos_service.services.bucketService import (
            check_bucket_has_contents,
            update_bucket_clean_status,
        )
        from cos_service.services.ibm_iam_service import get_iam_access_token

        try:
            update_bucket_clean_status(bucket["subscription_id"], Status.INPROGRESS, session)
            access_token = get_iam_access_token(api_key)

            has_contents = check_bucket_has_contents(access_token, bucket)
            if not has_contents:
                logging.info(f"{bucket['name']} is empty.")
                return True
            return False
        except Exception as e:
            update_bucket_clean_status(bucket["subscription_id"], Status.FAILED, session)
            state_manager.push_state({"clean_status": Status.FAILED.value})
            raise e

    @step
    def create_expiration_rule(
        api_key: str,
        bucket: dict,
        is_bucket_empty: bool,
        state_manager: StateManager = depends(state_manager_dependency),
        session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        # [À REPORTER] corps du fichier d'entreprise à partir de la ligne 138.
        raise NotImplementedError("create_expiration_rule : corps à reporter depuis le fichier d'entreprise")

    # [À REPORTER] étapes suivantes (sensor PokeReturnValue, retrait de la règle,
    # statut final) et enchaînement des étapes, lignes 138 et suivantes.


bucket_clean()
