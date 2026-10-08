"""Accès générique aux workspaces IBM Schematics : création, mise à jour, exécution.

Ce module ne connaît aucun produit. Les variables Terraform d'un bucket sont
construites par les services métier (``workspaceService``), pas ici.

[RECONSTITUTION] Le bloc d'imports était replié sur la photo : les imports
ci-dessous sont déduits des symboles utilisés (``SchematicsBackend``, ``VCS``,
``OrchestratorEnvironment``, ``constants``). À vérifier contre l'original.
"""
import logging
import os
import re
import time
from typing import NamedTuple

from bp2i_airflow_library.config import OrchestratorEnvironment
from bp2i_airflow_library.dependencies import SchematicsBackend
from bp2i_terraform.backends.schematics import VCS

from cos_service.utils import constants

logger = logging.getLogger(__name__)

# Version Terraform : une seule source, les DAGs dérivent leur tf_directory
# de TERRAFORM_VERSION au lieu de coder "v1.12" de leur côté.
TERRAFORM_VERSION = "1.12"
TF_VERSION_LABEL = f"terraform_v{TERRAFORM_VERSION}"
SCHEMATICS_PROJECT = "rg-realms"

# Branche git clonée par Schematics et niveau TF_LOG, par environnement.
# Alignés sur les branches de la CI (.gitlab-ci.yml) : main -> int,
# preprod -> pprod, prod -> prod. Ces défauts ne se changent pas dans le code.
DEFAULT_BRANCHES = {
    OrchestratorEnvironment.INT.value: "main",
    OrchestratorEnvironment.PREPROD.value: "preprod",
    OrchestratorEnvironment.PROD.value: "prod",
}
# Tags posés sur le workspace Schematics (le tag d'environnement garde le
# libellé historique "pprod" pour la préprod).
ENV_TAGS = {
    OrchestratorEnvironment.INT.value: "env:int",
    OrchestratorEnvironment.PREPROD.value: "env:pprod",
    OrchestratorEnvironment.PROD.value: "env:prod",
}
# TF_LOG : TRACE, DEBUG, INFO, WARN, ERROR. DEBUG trace les requêtes HTTP des
# providers dans les logs Schematics : confortable en INT, à éviter en prod.
DEFAULT_TF_LOG = {
    OrchestratorEnvironment.INT.value: "DEBUG",
    OrchestratorEnvironment.PREPROD.value: "INFO",
    OrchestratorEnvironment.PROD.value: "ERROR",
}
# Règle première : Schematics clone LA BRANCHE SUR LAQUELLE LE DAG TOURNE.
# L'orchestrateur exécute le code du produit sur la branche donnée à la
# gateway (``product_branch``) ; le DAG et son Terraform vivent dans le même
# dépôt, ils doivent être à la même version. Voir docs/adr/0002.
# Ordre de résolution de la branche, à chaque appel (jamais à l'import) :
# 1. ``payload.product_branch`` de la demande, transmis par le DAG ;
# 2. Airflow Variable `cos_tf_branch` (roue de secours, INT seulement) ;
# 3. variable d'environnement COS_TF_BRANCH ;
# 4. défaut de l'environnement ci-dessus.
# En pprod et prod, toute branche autre que celle de l'environnement est
# refusée avant tout appel Schematics.
BRANCH_SETTING = "cos_tf_branch"
TF_LOG_SETTING = "cos_tf_log_level"


class EnvSettings(NamedTuple):
    tags: list[str]
    branch: str
    tf_log: str


def setting(name: str, default: str) -> str:
    """Valeur d'un réglage : Airflow Variable, sinon variable d'environnement, sinon défaut."""
    fallback = os.environ.get(name.upper(), default)
    try:
        from airflow.models import Variable
    except ImportError:
        return fallback
    try:
        return Variable.get(name, default_var=fallback)
    except Exception as exc:  # métadonnées Airflow injoignables : ne pas bloquer la demande
        logger.warning("could not read Airflow Variable %s (%s), using %r", name, exc, fallback)
        return fallback


def settings_for(orchestrator_env, product_branch: str | None = None) -> EnvSettings:
    """Tags, branche Terraform et TF_LOG d'un environnement (enum ou sa valeur).

    ``product_branch`` est la branche sur laquelle le DAG tourne
    (``payload.product_branch``) : Schematics clone la même. Sans elle, les surcharges puis le défaut de l'environnement
    s'appliquent. Lève une ``ValueError`` explicite sur un environnement
    inconnu, et en pprod/prod sur une branche autre que celle de
    l'environnement, au lieu d'envoyer une mauvaise branche à Schematics.
    """
    key = getattr(orchestrator_env, "value", orchestrator_env)
    if key not in DEFAULT_BRANCHES:
        raise ValueError(
            f"Unknown orchestrator environment '{key}', expected one of {sorted(DEFAULT_BRANCHES)}"
        )
    expected = DEFAULT_BRANCHES[key]
    branch = product_branch or setting(BRANCH_SETTING, expected)
    if key != OrchestratorEnvironment.INT.value and branch != expected:
        raise ValueError(
            f"Environment '{key}' only runs Terraform from branch '{expected}', got '{branch}'"
        )
    return EnvSettings(
        tags=[ENV_TAGS[key], "agent:ga", "version:1.0"],
        branch=branch,
        tf_log=setting(TF_LOG_SETTING, DEFAULT_TF_LOG[key]),
    )


def _vcs(settings: EnvSettings, tf_directory: str, gitlab_token: str) -> VCS:
    return VCS(
        repository=constants.TERRAFORM_REPOSITORY,
        branch=settings.branch,
        oauth_token_id=gitlab_token,
        directory=tf_directory,
    )


def _env_values(settings: EnvSettings) -> list[dict]:
    return [{"TF_LOG": settings.tf_log}]


def create_or_update_ws(
    tf: SchematicsBackend,
    workspace_name: str,
    orchestrator_env: str,
    tf_directory: str,
    variables: dict,
    description: str,
    gitlab_token: str,
    product_branch: str | None = None,
) -> dict:
    """Crée le workspace, ou le met à jour s'il existe déjà sous ce nom.

    ``product_branch`` : branche de la demande (``payload.product_branch``),
    clonée par Schematics à la place du défaut de l'environnement.
    """
    settings = settings_for(orchestrator_env, product_branch)
    create_ws_result = tf.workspaces.create_or_update(
        workspace_name,
        tags=settings.tags,
        tf_version=TF_VERSION_LABEL,
        vcs=_vcs(settings, tf_directory, gitlab_token),
        description=description,
        env_values=_env_values(settings),
        variables=variables,
        project=SCHEMATICS_PROJECT,
    )
    return dict(create_ws_result)


def update_ws(
    tf: SchematicsBackend,
    workspace_id: str,
    orchestrator_env: str,
    tf_directory: str,
    description: str,
    gitlab_token: str,
    product_branch: str | None = None,
):
    """Met à jour tags, VCS, description et env du workspace (pas ses variables)."""
    settings = settings_for(orchestrator_env, product_branch)
    return tf.workspaces.update(
        workspace_id=workspace_id,
        tags=settings.tags,
        tf_version=TF_VERSION_LABEL,
        vcs=_vcs(settings, tf_directory, gitlab_token),
        description=description,
        env_values=_env_values(settings),
    )


def update_ws_variables(tf: SchematicsBackend, workspace_id: str, variables: dict):
    """Remplace les variables du workspace.

    Seuls les noms sont loggués : le dict contient des ``TerraformVar``
    sensibles (tokens Vault) qui ne doivent jamais apparaître dans les logs.
    """
    logger.debug(
        "updating %d variables of workspace %s: %s", len(variables), workspace_id, sorted(variables)
    )
    return tf.workspaces.update_variables(workspace_id=workspace_id, variables=variables)


PLAN_SUMMARY_RE = re.compile(r"Plan:\s*(\d+) to add,\s*(\d+) to change,\s*(\d+) to destroy", re.IGNORECASE)
NO_CHANGES_RE = re.compile(r"No changes\.|Your infrastructure matches the configuration", re.IGNORECASE)


def _activity_logs(activity) -> dict:
    """Journaux d'une activité Schematics par template ; {} si l'objet n'en expose pas
    (ancienne version de bp2i_terraform pour ``plan``)."""
    get_logs = getattr(activity, "get_logs", None)
    if get_logs is None:
        return {}
    try:
        return dict(get_logs() or {})
    except Exception as exc:  # les logs ne doivent jamais faire échouer le run
        logger.warning("could not read Schematics activity logs: %s", exc)
        return {}


def plan_summary(template_logs: str) -> str | None:
    """La ligne ``Plan: X to add, Y to change, Z to destroy`` ou ``No changes`` du plan."""
    match = PLAN_SUMMARY_RE.search(template_logs)
    if match:
        add, change, destroy = match.groups()
        return f"{add} to add, {change} to change, {destroy} to destroy"
    if NO_CHANGES_RE.search(template_logs):
        return "no changes"
    return None


def _log_activity(kind: str, workspace_id: str, activity) -> dict:
    """Journalise les logs Terraform d'une activité, avec un résumé du plan et la durée."""
    logs = _activity_logs(activity)
    for template_id, template_logs in logs.items():
        logger.info("%s logs of workspace %s, template %s:\n%s", kind, workspace_id, template_id, template_logs)
        summary = plan_summary(template_logs)
        if summary:
            logger.info("%s of workspace %s, template %s: %s", kind, workspace_id, template_id, summary)
        match = PLAN_SUMMARY_RE.search(template_logs)
        if kind == "plan" and match and int(match.group(3)) > 0:
            logger.warning("plan of workspace %s destroys %s resource(s)", workspace_id, match.group(3))
    return logs


def run_workspace(tf: SchematicsBackend, workspace_id: str) -> dict:
    """Plan puis apply, et renvoie les outputs Terraform du premier template.

    Journalise les logs du plan (avec son résumé ajouts / changements /
    destructions, et un warning s'il détruit), puis ceux de l'apply, la durée
    de chaque phase et les noms des outputs. Lève une ``RuntimeError`` lisible
    si l'apply n'a produit aucun output, au lieu d'un ``IndexError`` anonyme.
    """
    workspace = tf.workspaces.get_by_id(workspace_id)
    logger.info("plan of workspace %s (%s) starting", workspace_id, getattr(workspace, "name", "?"))
    started = time.monotonic()
    plan_activity = workspace.plan()
    _log_activity("plan", workspace_id, plan_activity)
    logger.info("plan of workspace %s done in %.0f s", workspace_id, time.monotonic() - started)

    started = time.monotonic()
    apply_activity = workspace.apply()
    _log_activity("apply", workspace_id, apply_activity)
    logger.info("apply of workspace %s done in %.0f s", workspace_id, time.monotonic() - started)

    outputs = workspace.get_outputs()
    if not outputs or not outputs[0].output_values:
        raise RuntimeError(f"workspace {workspace_id} produced no Terraform outputs after apply")
    values = dict(outputs[0].output_values[0])
    logger.info("outputs of workspace %s: %s", workspace_id, sorted(values))  # noms seulement, jamais les valeurs
    return values


_setting = setting  # ancien nom, gardé pour les tests et les appels existants
