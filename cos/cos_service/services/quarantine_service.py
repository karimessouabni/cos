"""Quarantaine d'un bucket pendant un clean (ADR 0003 et 0004).

Une règle Context-Based Restrictions (CBR) sur le bucket **bloque tout** :
clients, Airflow et Schematics. Aucune zone CBR ne peut laisser passer
l'orchestrateur sans laisser passer les clients, car ils arrivent chez COS par
le même VPE (ADC). L'orchestrateur **ouvre** donc la quarantaine quelques
minutes quand il doit agir sur le bucket (règle désactivée, puis attente du
retour de l'accès), puis la **referme** (règle réactivée).

La règle vit dans un **workspace Schematics séparé** du bucket
(``terraform/v1.12/bucket_quarantine``, nom ``ws_cbr_bucket_<subscription>``),
dont le state ne contient que la zone et la règle. Son refresh n'appelle que
l'API CBR, qu'une règle sur COS ne bloque jamais : il reste pilotable quoi que
la règle bloque, et la lever est toujours possible. Le bucket n'y est jamais
lu, son nom et son instance passent en variables depuis la base.

Cycle de vie : ``set`` crée le workspace, règle active ; ``open`` / ``close``
désactivent puis réactivent la règle ; ``lift`` détruit la zone, la règle et
le workspace (fin du clean, annulation, suppression du bucket).
"""
import logging
import time

from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_terraform.components.cooldown_policies import LinearCooldownPolicy

from cos_service.services.schematics_service import TERRAFORM_VERSION, create_or_update_ws, run_workspace

logger = logging.getLogger(__name__)

QUARANTINE_TF_DIRECTORY = f"terraform/v{TERRAFORM_VERSION}/bucket_quarantine"
# Réglage (Airflow Variable, sinon variable d'environnement, sinon défaut) :
# mode CBR, ``report`` pour valider sur les premiers clients, puis ``enabled``.
ENFORCEMENT_SETTING = "cos_quarantine_enforcement_mode"
DEFAULT_ENFORCEMENT = "enabled"
# Propagation d'une règle CBR : quelques minutes. Après une ouverture, le
# bucket est sondé toutes les 30 s, 20 min au plus, jusqu'au retour de l'accès.
PROBE_INTERVAL_SECONDS = 30
PROBE_MAX_ATTEMPTS = 40
HTTP_OK = 200
# Destruction des ressources : Schematics est interrogé toutes les 5 s, au plus 120 fois (10 min).
DESTROY_POLL_DELAY_SECONDS = 5
DESTROY_MAX_ATTEMPTS = 120
SCHEMATICS_NOT_FOUND = 404


def quarantine_workspace_name(subscription_id: str) -> str:
    return f"ws_cbr_bucket_{subscription_id}"


def quarantine_settings() -> dict:
    from cos_service.services.schematics_service import setting

    return {"enforcement_mode": setting(ENFORCEMENT_SETTING, DEFAULT_ENFORCEMENT)}


def _realm_account(realm: dict, key: str) -> str:
    """Identifiant IBM d'un compte du realm (modèle du reader : ``wklapp_account_id``).
    Lève si absent : la zone et la règle CBR appartiennent à ce compte."""
    value = str(realm.get(key) or "").strip()
    if not value:
        raise ValueError(f"realm {realm.get('name')!r} has no {key}: the quarantine cannot build its CBR zone")
    return value


def _tf_bool(value) -> str:
    """Les variables Schematics sont des chaînes ; Terraform convertit "true"/"false" en bool."""
    return "true" if value else "false"


def quarantine_variables(bucket: dict, secrets: dict, realm: dict, active: bool = True) -> dict:
    """Variables du workspace : le bucket vient de la base, jamais de COS.
    ``active`` : règle appliquée (bucket fermé) ou désactivée (bucket ouvert)."""
    from bp2i_terraform.backends.schematics import TerraformVar

    return {
        "bucket_name": bucket["name"],
        "cos_instance_crn": bucket["cos"]["crn"],
        "region": bucket["region"],
        "app_code": bucket["cos"]["context"]["app_code"],
        "wklapp_account_id": realm.get("wklapp_account_number"),  # chemin Vault, comme le module bucket
        "cbr_account_id": _realm_account(realm, "wklapp_account_id"),  # propriétaire de la zone et de la règle
        "orchestrator_environment": ENVIRONMENT,
        "vault_read_addr": secrets["vault_read_addr"],
        "vault_read_token": TerraformVar(secrets["vault_read_token"], True),
        "rule_active": _tf_bool(active),
        **quarantine_settings(),
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


def _apply_rule(*, tf, bucket: dict, workspace_id: str, vault, reader, active: bool) -> None:
    """Réécrit les variables du workspace (tokens Vault frais) avec la règle
    active ou non, puis applique. Ne passe jamais par COS."""
    from cos_service.services.schematics_service import update_ws_variables

    secrets, realm = _workspace_context(bucket, vault, reader)
    update_ws_variables(tf, workspace_id, quarantine_variables(bucket, secrets, realm, active=active))
    outputs = run_workspace(tf, workspace_id)
    logger.info("quarantine rule of bucket %s now %s", bucket["name"], _output(outputs, "enforcement_mode"))


def probe_bucket_until(access_token: str, bucket: dict, expected: int,
                       sleep=None, interval: int | None = None, attempts: int | None = None) -> dict:
    """Liste le bucket jusqu'au statut HTTP attendu. Ne lève jamais : le compte
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


def open_bucket_quarantine(*, tf, bucket: dict, workspace_id: str, vault, reader, access_token: str) -> dict:
    """Désactive la règle et attend que le bucket réponde 200 depuis Airflow.
    Lève si l'accès ne revient pas : agir sur un bucket encore bloqué lirait un
    403 comme une réponse. La quarantaine est alors refermée avant de lever, le
    bucket reste du côté sûr."""
    _apply_rule(tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader, active=False)
    result = probe_bucket_until(access_token, bucket, HTTP_OK)
    if not result["reached"]:
        logger.error("bucket %s still answers HTTP %s after the quarantine was opened, closing it again",
                     bucket["name"], result["status"])
        close_bucket_quarantine(tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader)
        raise RuntimeError(
            f"the bucket {bucket['name']} still answers HTTP {result['status']} {result['seconds']} s after "
            "its quarantine rule was disabled; the rule is active again"
        )
    logger.info("quarantine of bucket %s open after %s s", bucket["name"], result["seconds"])
    return result


def close_bucket_quarantine(*, tf, bucket: dict, workspace_id: str, vault, reader) -> None:
    """Réactive la règle : le bucket est de nouveau fermé, après propagation."""
    _apply_rule(tf=tf, bucket=bucket, workspace_id=workspace_id, vault=vault, reader=reader, active=True)


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
