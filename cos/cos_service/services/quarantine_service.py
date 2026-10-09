"""Quarantaine d'un bucket pendant la période de grâce d'un clean (v2).

Une règle Context-Based Restrictions (CBR) sur le bucket ne laisse passer que
Schematics du compte hub (référence de service) : le client est bloqué, et
l'orchestrateur regarde le bucket depuis le workspace de quarantaine lui-même
(``probe_bucket_via_schematics``), bucket toujours fermé. Le compte hub vient
du realm (``buhub_account_id``) ; sans lui, pas de quarantaine.

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
# Réglage (Airflow Variable, sinon variable d'environnement, sinon défaut) :
# mode CBR, ``report`` pour valider sur les premiers clients, puis ``enabled``.
ENFORCEMENT_SETTING = "cos_quarantine_enforcement_mode"
DEFAULT_ENFORCEMENT = "enabled"
# URL du bucket pour la sonde depuis Schematics (https://<host>/<bucket>).
# Surcharge seulement : sans elle, la sonde prend l'endpoint du bucket en base
# (``virtual_server_endpoint``, celui du clean) ; à défaut, le module construit
# l'endpoint privé de la région.
PROBE_ENDPOINT_SETTING = "cos_quarantine_probe_endpoint"
# Destruction des ressources : Schematics est interrogé toutes les 5 s, au plus 120 fois (10 min).
DESTROY_POLL_DELAY_SECONDS = 5
DESTROY_MAX_ATTEMPTS = 120
SCHEMATICS_NOT_FOUND = 404


def quarantine_workspace_name(subscription_id: str) -> str:
    return f"ws_cbr_bucket_{subscription_id}"


def quarantine_settings() -> dict:
    from cos_service.services.schematics_service import setting

    return {
        "enforcement_mode": setting(ENFORCEMENT_SETTING, DEFAULT_ENFORCEMENT),
        "probe_endpoint": setting(PROBE_ENDPOINT_SETTING, ""),
    }



def _realm_account(realm: dict, key: str) -> str:
    """Identifiant IBM d'un compte du realm (modèle du reader : ``buhub_account_id``,
    ``wklapp_account_id``). Lève si absent : la zone CBR ne laisserait passer personne."""
    value = str(realm.get(key) or "").strip()
    if not value:
        raise ValueError(f"realm {realm.get('name')!r} has no {key}: the quarantine cannot build its CBR zone")
    return value


def _tf_bool(value) -> str:
    """Les variables Schematics sont des chaînes ; Terraform convertit "true"/"false" en bool."""
    return "true" if value else "false"


def quarantine_variables(bucket: dict, secrets: dict, realm: dict, probe: bool = False) -> dict:
    """Variables du workspace : le bucket vient de la base, jamais de COS.
    ``probe`` active le listing du bucket depuis Schematics (probe.tf)."""
    from bp2i_terraform.backends.schematics import TerraformVar

    settings = quarantine_settings()
    # Le réglage est une URL complète, donc propre à un bucket : il ne sert que
    # de surcharge. Par défaut, l'endpoint du bucket lui-même.
    settings["probe_endpoint"] = settings["probe_endpoint"] or bucket.get("virtual_server_endpoint") or ""

    return {
        "bucket_name": bucket["name"],
        "cos_instance_crn": bucket["cos"]["crn"],
        "region": bucket["region"],
        "app_code": bucket["cos"]["context"]["app_code"],
        "wklapp_account_id": realm.get("wklapp_account_number"),  # chemin Vault, comme le module bucket
        "cbr_account_id": _realm_account(realm, "wklapp_account_id"),  # propriétaire de la zone et de la règle
        "hub_account_id": _realm_account(realm, "buhub_account_id"),  # seul contexte que la règle laisse passer
        "orchestrator_environment": ENVIRONMENT,
        "vault_read_addr": secrets["vault_read_addr"],
        "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
        "probe_enabled": _tf_bool(probe),
        "probe_versions": _tf_bool(bucket.get("object_versioning_enabled")),
        **settings,
    }


def _workspace_context(bucket: dict, vault, reader) -> tuple[dict, dict]:
    """Secrets Vault et realm du bucket, nécessaires à chaque mise à jour des variables."""
    from cos_service.services.contextService import get_realm
    from cos_service.services.vault_service import get_vault_secrets

    context = bucket["cos"]["context"]
    secrets = get_vault_secrets(realm_name=context["realm"], apcode=context["app_code"], vault=vault, reader=reader)
    return secrets, get_realm(reader).model_dump()


def _output(outputs: dict, name: str):
    return (outputs.get(name) or {}).get("value")


def set_bucket_quarantine(*, tf, bucket: dict, payload, vault, reader) -> str:
    """Crée (ou met à jour) le workspace de quarantaine du bucket et l'applique.
    Renvoie l'identifiant du workspace, à garder en base pour la levée."""
    from cos_service.services.schematics_service import update_ws_variables

    secrets, realm = _workspace_context(bucket, vault, reader)
    workspace_name = quarantine_workspace_name(bucket["subscription_id"])
    variables = quarantine_variables(bucket, secrets, realm)
    logger.info("setting quarantine on bucket %s (workspace %s)", bucket["name"], workspace_name)
    created = create_or_update_ws(
        tf,
        workspace_name=workspace_name,
        orchestrator_env=ENVIRONMENT,
        tf_directory=QUARANTINE_TF_DIRECTORY,
        variables=variables,
        description=f"Quarantine of bucket {bucket['name']} during the clean grace period",
        gitlab_token=secrets["gitlab_token"],
        product_branch=getattr(payload, "product_branch", None),
    )
    workspace_id = created["id"]
    # Un workspace retrouvé par son nom (run précédent) garde ses anciennes variables :
    # le jeu complet est réécrit pour qu'aucune variable ajoutée depuis ne manque au plan.
    update_ws_variables(tf, workspace_id, variables)
    outputs = run_workspace(tf, workspace_id)
    logger.info("quarantine of bucket %s set: CBR rule %s", bucket["name"], _output(outputs, "cbr_rule_id"))
    return workspace_id


def probe_bucket_via_schematics(*, tf, bucket: dict, workspace_id: str, vault, reader) -> dict:
    """Liste le bucket depuis le workspace de quarantaine (seul endroit qui passe
    la règle) : active la sonde dans les variables, ré-applique, lit les sorties.
    Renvoie ``status`` (HTTP vu par Schematics) et ``empty`` (None si refusé).
    La sonde reste active : chaque apply suivant la relit."""
    from cos_service.services.schematics_service import update_ws_variables

    secrets, realm = _workspace_context(bucket, vault, reader)
    update_ws_variables(tf, workspace_id, quarantine_variables(bucket, secrets, realm, probe=True))
    outputs = run_workspace(tf, workspace_id)
    status = _output(outputs, "probe_status_code")
    result = {"status": int(status) if status is not None else None, "empty": _output(outputs, "bucket_empty")}
    logger.info("probe of bucket %s from Schematics: HTTP %s, empty=%s", bucket["name"], result["status"], result["empty"])
    return result


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
