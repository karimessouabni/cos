"""Buckets : persistance SQLAlchemy et appels S3 directs sur le bucket.

[RECONSTITUTION] Reconstitué depuis les captures PyCharm (355 lignes, toutes
visibles). Corrections apportées par rapport à l'original, voir le commit :
recherche exacte par subscription_id, en-têtes et appels HTTP factorisés avec
timeout et contrôle du statut, plus de ``print``, imports SQLAlchemy au niveau
module, version Terraform tirée de ``schematics_service``.

Contrat des fonctions d'écriture : ``process_bucket_creation`` attend les
LIGNES ORM ``cos`` et ``backup_vault`` (affectation de relations), pas les
dicts sérialisés qui circulent entre les étapes du DAG.
"""
import base64
import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

import requests
from sqlalchemy import update
from sqlalchemy.orm import joinedload

import cos_service.utils.constants as constants
from bp2i_airflow_library.dependencies import SASession
from cos_service.models.Bucket import Bucket
from cos_service.models.Cos import Cos
from cos_service.models.Workspace import Workspace
from cos_service.schemas.action import Action
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus
from cos_service.services.schematics_service import TF_VERSION_LABEL

logger = logging.getLogger(__name__)

S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
S3_NAMESPACES = {"s3": S3_NAMESPACE}
# L'original appelait les endpoints S3 "direct" avec verify=False. Centralisé
# ici pour pouvoir réactiver la vérification TLS en un seul endroit.
S3_VERIFY_TLS = False
S3_TIMEOUT_SECONDS = 30
SCHEMATICS_WORKSPACE_LOCATION = "eu-fr2"

CLEAN_BUCKET_LIFECYCLE_CONFIGURATION = """
    <LifecycleConfiguration>
        <Rule>
            <ID>clean_bucket</ID>
            <Filter><Prefix/>
            </Filter>
            <Status>Enabled</Status>
            <NoncurrentVersionExpiration>
                <NoncurrentDays>1</NoncurrentDays>
            </NoncurrentVersionExpiration>
            <AbortIncompleteMultipartUpload>
                <DaysAfterInitiation>1</DaysAfterInitiation>
            </AbortIncompleteMultipartUpload>
            <Expiration>
                <Days>1</Days>
            </Expiration>
        </Rule>
    </LifecycleConfiguration>
"""


# --- lecture ------------------------------------------------------------------

def _bucket_relations():
    """Relations que ``to_dict()`` doit trouver chargées.

    Les DAGs lisent ``bucket["workspace"]["workspace_id"]``, ``bucket["cos"]["crn"]``
    et ``bucket["backup_vault"]`` sur le dict renvoyé : les relations doivent être
    chargées AVANT ``to_dict()``. L'original le faisait par effet de bord avec
    cinq ``logger.info(bucket.workspace)`` ; ici c'est explicite.
    """
    return (
        joinedload(Bucket.workspace),
        joinedload(Bucket.cos).joinedload(Cos.workspace),
        joinedload(Bucket.cos).joinedload(Cos.context),
        joinedload(Bucket.backup_vault),
    )


def get_bucket_by_sub_id(session: SASession, subscription_id: str) -> Optional[dict]:
    """Bucket par subscription_id, en correspondance exacte, relations chargées.

    L'original faisait ``contains(subscription_id)`` + ``first()`` : un id qui
    est une sous-chaîne d'un autre pouvait renvoyer le mauvais bucket.
    """
    bucket = (
        session.query(Bucket)
        .options(*_bucket_relations())
        .filter(Bucket.subscription_id == subscription_id)
        .one_or_none()
    )
    return bucket.to_dict() if bucket else None


def get_bucket_by_name(session: SASession, name: str) -> Optional[dict]:
    bucket = (
        session.query(Bucket)
        .options(*_bucket_relations())
        .filter(Bucket.name == name)
        .one_or_none()
    )
    return bucket.to_dict() if bucket else None


def get_buckets_with_expired_clean_policy(session: SASession) -> list:
    now = datetime.now()
    bucket_list = (
        session.query(Bucket)
        .filter(
            Bucket.has_expiration_rule.is_(True),
            Bucket.expiration_rule_created_at + timedelta(days=1) <= now,
        )
        .options(joinedload(Bucket.cos).joinedload(Cos.context))
        .all()
    )
    return [bucket.to_dict() for bucket in bucket_list]


# --- appels S3 sur le bucket --------------------------------------------------------

def _s3_headers(access_token: str, cos_instance_crn: str | None = None, **extra) -> dict:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    if cos_instance_crn:
        headers["Resource-Crn"] = cos_instance_crn
    headers.update(extra)
    return headers


def _s3_get_xml(url: str, headers: dict, not_found_ok: bool = False) -> Optional[ET.Element]:
    """GET puis parse XML. ``not_found_ok`` : un 404 renvoie None au lieu de lever.

    Les autres statuts d'erreur lèvent : l'original parsait la réponse d'erreur
    comme si c'était la ressource, et un 403 passait pour "pas de contenu".
    """
    response = requests.get(url, headers=headers, verify=S3_VERIFY_TLS, timeout=S3_TIMEOUT_SECONDS)
    if not_found_ok and response.status_code == 404:
        return None
    response.raise_for_status()
    return ET.fromstring(response.content)


def check_bucket_has_contents(access_token: str, bucket: dict) -> bool:
    """Vrai si le bucket contient au moins un objet (une seule clé demandée)."""
    headers = _s3_headers(access_token, bucket["cos"]["crn"])
    root = _s3_get_xml(f'{bucket["virtual_server_endpoint"]}?max-keys=1', headers)
    return len(root.findall("s3:Contents", S3_NAMESPACES)) > 0


def is_versioning_enabled(access_token: str, bucket_vpe: str) -> bool:
    root = _s3_get_xml(f"{bucket_vpe}?versioning", _s3_headers(access_token))
    status = root.find("s3:Status", S3_NAMESPACES)
    logger.info("versioning status of %s: %s", bucket_vpe, None if status is None else status.text)
    return status is not None and status.text == "Enabled"


def is_object_lock_enabled(access_token: str, bucket_vpe: str) -> bool:
    root = _s3_get_xml(f"{bucket_vpe}?object-lock", _s3_headers(access_token), not_found_ok=True)
    if root is None:
        return False
    status = root.find("s3:ObjectLockEnabled", S3_NAMESPACES)
    logger.info("object lock status of %s: %s", bucket_vpe, None if status is None else status.text)
    return status is not None and status.text == "Enabled"


def create_expiration_rule(access_token: str, bucket: dict) -> bool:
    """Pose la règle de cycle de vie qui vide le bucket en un jour."""
    body = CLEAN_BUCKET_LIFECYCLE_CONFIGURATION.encode("utf-8")
    content_md5 = base64.b64encode(hashlib.md5(body).digest()).decode("utf-8")
    headers = _s3_headers(
        access_token,
        bucket["cos"]["crn"],
        **{"Accept": "application/*", "Content-MD5": content_md5},
    )
    url = f'{bucket["virtual_server_endpoint"]}?lifecycle'

    response = requests.put(url, headers=headers, data=body, verify=S3_VERIFY_TLS, timeout=S3_TIMEOUT_SECONDS)
    logger.info("lifecycle configuration on %s: %s %s", url, response.status_code, response.text)
    response.raise_for_status()
    return True


def delete_lifecycle_policy(access_token: str, bucket: dict) -> int:
    headers = _s3_headers(access_token, bucket["cos"]["crn"], Accept="application/*")
    url = f'{bucket["virtual_server_endpoint"]}?lifecycle'

    response = requests.delete(url, headers=headers, verify=S3_VERIFY_TLS, timeout=S3_TIMEOUT_SECONDS)
    logger.info("lifecycle deletion on %s: %s", url, response.status_code)
    return response.status_code


# --- écriture -----------------------------------------------------------------------

def _execute(session: SASession, statement) -> None:
    session.execute(statement)
    session.commit()


def process_bucket_creation(
    payload,
    realm,
    immutability: dict,
    account_instances_crn: dict,
    description,
    cos,
    backup_vault,
    session: SASession,
) -> dict:
    """Insère la ligne bucket et son workspace. ``cos`` et ``backup_vault`` sont
    des lignes ORM (relations), ``backup_vault`` pouvant être None."""
    bucket = Bucket(
        subscription_id=payload.subscription_id,
        description=description,
        status=SubscriptionStatus.CREATING.value,
        created_at=datetime.today(),
        created_by=payload.requestor,
        region=payload.region,
        storage_class=payload.storage_class,
        activity_tracker_crn=account_instances_crn.get("cloudlogs", None),
        monitoring_crn=account_instances_crn.get("cloudlogs", None),
        kms_crn=account_instances_crn.get("encryption_key", None),
        # Remplacé par la vraie URL dans complete_bucket_create, après l'apply.
        virtual_server_endpoint="vip",
        environment=payload.environment,
        retention_enabled=immutability["retention"]["retention_enabled"] or False,
        retention_default=immutability["retention"]["default"],
        retention_minimum=immutability["retention"]["minimum"],
        retention_maximum=immutability["retention"]["maximum"],
        enable_custom_permissions=payload.enable_custom_permissions,
        object_lock_duration_days=immutability["object_lock_duration_days"],
        object_lock_duration_years=immutability["object_lock_duration_years"],
        has_expiration_rule=False,
        object_versioning_enabled=immutability["object_versioning_enabled"],
        backup_enabled=immutability["backup"]["backup_enabled"] or False,
        backup_retention_days=immutability["backup"]["backup_retention_days"],
    )

    workspace = Workspace(
        action=Action.APPLY.value,
        location=SCHEMATICS_WORKSPACE_LOCATION,
        version_type=TF_VERSION_LABEL,
        status=Status.INPROGRESS.value,
        type="terraform",
        git_repo=constants.TERRAFORM_REPOSITORY,
    )

    bucket.cos = cos
    bucket.workspace = workspace
    bucket.backup_vault = backup_vault
    session.add(bucket)
    session.commit()
    return dict(bucket)


def update_bucket_workspace_details(bucket: dict, create_ws_result: dict, session: SASession) -> None:
    _execute(
        session,
        update(Workspace)
        .values(
            name=create_ws_result["name"],
            workspace_id=create_ws_result["id"],
            status=Status.INPROGRESS.value,
        )
        .where(Workspace.bucket_subscription_id == bucket["subscription_id"]),
    )


def update_bucket_status(subscription_id: str, subscription_status: SubscriptionStatus, session: SASession) -> None:
    _execute(
        session,
        update(Bucket)
        .values(status=subscription_status.value)
        .where(Bucket.subscription_id == subscription_id),
    )


def update_bucket_clean_status(subscription_id: str, clean_status: Status, session: SASession) -> None:
    _execute(
        session,
        update(Bucket)
        .values(clean_status=clean_status.value)
        .where(Bucket.subscription_id == subscription_id),
    )


def update_bucket_workspace_status(subscription_id: str, status: Status, session: SASession) -> None:
    _execute(
        session,
        update(Workspace)
        .values(status=status.value)
        .where(Workspace.bucket_subscription_id == subscription_id),
    )


def complete_bucket_create(subscription_id: str, vip: str, apply_tf_result: dict, session: SASession) -> None:
    session.execute(
        update(Bucket)
        .values(
            name=apply_tf_result["bucket_name"]["value"],
            status=SubscriptionStatus.ACTIVE.value,
            action=Action.APPLY.value,
            bucket_crn=apply_tf_result["bucket_crn"]["value"],
            virtual_server_endpoint=vip,
        )
        .where(Bucket.subscription_id == subscription_id)
    )
    session.execute(
        update(Workspace)
        .values(status=Status.SUCCESS.value)
        .where(Workspace.bucket_subscription_id == subscription_id)
    )
    session.commit()


def process_bucket_update(
    subscription_id: str,
    immutability: dict,
    enable_custom_permissions: bool,
    description: str,
    session: SASession,
) -> None:
    from cos_service.services.backup_vault_service import get_backup_vault_by_sub_id

    backup_vault_sub_id = immutability["backup"]["backup_vault_sub_id"]
    backup_vault = (
        get_backup_vault_by_sub_id(backup_vault_sub_id, session) if backup_vault_sub_id is not None else None
    )
    _execute(
        session,
        update(Bucket)
        .values(
            retention_enabled=immutability["retention"]["retention_enabled"],
            retention_default=immutability["retention"]["default"],
            retention_minimum=immutability["retention"]["minimum"],
            retention_maximum=immutability["retention"]["maximum"],
            object_lock_duration_days=immutability["object_lock_duration_days"],
            object_lock_duration_years=immutability["object_lock_duration_years"],
            object_versioning_enabled=immutability["object_versioning_enabled"],
            enable_custom_permissions=enable_custom_permissions,
            backup_enabled=immutability["backup"]["backup_enabled"],
            backup_vault_subscription_id=backup_vault.subscription_id if backup_vault is not None else None,
            backup_retention_days=immutability["backup"]["backup_retention_days"],
            description=description,
        )
        .where(Bucket.subscription_id == subscription_id),
    )


def update_bucket_on_destroy(subscription_id: str, session: SASession) -> None:
    _execute(
        session,
        update(Workspace)
        .values(action=Action.DESTROY.value, status=Status.INPROGRESS.value)
        .where(Workspace.bucket_subscription_id == subscription_id),
    )
