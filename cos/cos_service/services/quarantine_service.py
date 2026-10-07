"""Quarantaine d'un bucket pendant la période de grâce d'un clean (v2).

Une règle Context-Based Restrictions (CBR) sur le bucket n'autorise plus que
les endpoints ``direct`` / ``private`` depuis la zone réseau de
l'orchestrateur : les clients, sur l'URL publique (clé API ou HMAC, proxy ou
non), reçoivent 403 ; Airflow (endpoint direct) et Schematics (référence de
service) continuent de passer.

La règle vit dans un **workspace Schematics séparé** du bucket
(``terraform/v1.12/bucket_quarantine``, nom ``ws_cbr_bucket_<subscription>``),
dont le state ne contient que la zone et la règle. Son refresh n'appelle que
l'API CBR, qu'une règle sur COS ne bloque jamais : il reste pilotable quoi que
la règle bloque, et la lever est toujours possible. Le bucket n'y est jamais
lu, son nom et son instance passent en variables depuis la base.

Cycle de vie court : créé à la mise en quarantaine, ressources détruites puis
workspace supprimé à la levée (fin du clean ou annulation). Voir
docs/adr/0003-periode-de-grace-du-clean.md.
"""
import logging

from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_terraform.components.cooldown_policies import LinearCooldownPolicy

from cos_service.services.schematics_service import TERRAFORM_VERSION, create_or_update_ws, run_workspace

logger = logging.getLogger(__name__)

QUARANTINE_TF_DIRECTORY = f"terraform/v{TERRAFORM_VERSION}/bucket_quarantine"
# Réglages (Airflow Variable, sinon variable d'environnement, sinon défaut) :
# VPC de l'orchestrateur autorisés, séparés par des virgules, et mode CBR.
ALLOWED_VPCS_SETTING = "cos_quarantine_allowed_vpc_crns"
ENFORCEMENT_SETTING = "cos_quarantine_enforcement_mode"
DEFAULT_ENFORCEMENT = "enabled"
# Destruction des ressources : Schematics est interrogé toutes les 5 s, au plus 120 fois (10 min).
DESTROY_POLL_DELAY_SECONDS = 5
DESTROY_MAX_ATTEMPTS = 120
SCHEMATICS_NOT_FOUND = 404


def quarantine_workspace_name(subscription_id: str) -> str:
    return f"ws_cbr_bucket_{subscription_id}"


def quarantine_settings() -> dict:
    from cos_service.services.schematics_service import setting

    vpcs = [v.strip() for v in setting(ALLOWED_VPCS_SETTING, "").split(",") if v.strip()]
    return {"allowed_vpc_crns": vpcs, "enforcement_mode": setting(ENFORCEMENT_SETTING, DEFAULT_ENFORCEMENT)}


def quarantine_variables(bucket: dict, secrets: dict, realm: dict) -> dict:
    """Variables du workspace : le bucket vient de la base, jamais de COS."""
    from bp2i_terraform.backends.schematics import TerraformVar

    return {
        "bucket_name": bucket["name"],
        "cos_instance_crn": bucket["cos"]["crn"],
        "region": bucket["region"],
        "app_code": bucket["cos"]["context"]["app_code"],
        "wklapp_account_id": realm.get("wklapp_account_number"),
        "orchestrator_environment": ENVIRONMENT,
        "vault_read_addr": secrets["vault_read_addr"],
        "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
        **quarantine_settings(),
    }


def set_bucket_quarantine(*, tf, bucket: dict, payload, vault, reader) -> str:
    """Crée (ou met à jour) le workspace de quarantaine du bucket et l'applique.
    Renvoie l'identifiant du workspace, à garder en base pour la levée."""
    from cos_service.services.contextService import get_realm
    from cos_service.services.vault_service import get_vault_secrets

    context = bucket["cos"]["context"]
    secrets = get_vault_secrets(realm_name=context["realm"], apcode=context["app_code"], vault=vault, reader=reader)
    realm = get_realm(reader).model_dump()
    workspace_name = quarantine_workspace_name(bucket["subscription_id"])
    logger.info("setting quarantine on bucket %s (workspace %s)", bucket["name"], workspace_name)
    created = create_or_update_ws(
        tf,
        workspace_name=workspace_name,
        orchestrator_env=ENVIRONMENT,
        tf_directory=QUARANTINE_TF_DIRECTORY,
        variables=quarantine_variables(bucket, secrets, realm),
        description=f"Quarantine of bucket {bucket['name']} during the clean grace period",
        gitlab_token=secrets["gitlab_token"],
        product_branch=getattr(payload, "product_branch", None),
    )
    workspace_id = created["id"]
    outputs = run_workspace(tf, workspace_id)
    logger.info("quarantine of bucket %s set: CBR rule %s", bucket["name"], (outputs.get("cbr_rule_id") or {}).get("value"))
    return workspace_id


def _is_not_found(exc: Exception) -> bool:
    return getattr(exc, "code", None) == SCHEMATICS_NOT_FOUND


def lift_bucket_quarantine(*, tf, workspace_id: str) -> None:
    """Détruit la zone et la règle, puis supprime le workspace. Un workspace
    déjà disparu (404) compte comme levé. Son state ne contient que des
    ressources CBR : cet appel ne passe jamais par COS."""
    try:
        tf.workspaces.get_by_id(workspace_id=workspace_id)  # lève avec code 404 si déjà supprimé
        tf.workspaces.delete_workspace_resources(
            workspace_id,
            cooldown_policy=LinearCooldownPolicy(delay=DESTROY_POLL_DELAY_SECONDS, max_attempts=DESTROY_MAX_ATTEMPTS),
        )
        tf.workspaces.get_by_id(workspace_id=workspace_id).delete()
    except Exception as exc:
        if not _is_not_found(exc):
            raise
        logger.info("quarantine workspace %s already gone (404)", workspace_id)
        return
    logger.info("quarantine workspace %s destroyed and deleted", workspace_id)
