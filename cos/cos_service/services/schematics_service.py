"""Accès générique aux workspaces IBM Schematics : création, mise à jour, exécution.

Ce module ne connaît aucun produit. Les variables Terraform d'un bucket sont
construites par les services métier (``workspaceService``), pas ici.

[RECONSTITUTION] Le bloc d'imports était replié sur la photo : les imports
ci-dessous sont déduits des symboles utilisés (``SchematicsBackend``, ``VCS``,
``OrchestratorEnvironment``, ``constants``). À vérifier contre l'original.
"""
import logging
import os
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
# Surcharges, lues à chaque appel (jamais à l'import), dans cet ordre :
# 1. Airflow Variable du même nom (interface Airflow de l'environnement, sans
#    redéploiement : par exemple `cos_tf_branch = feature/xxx` sur l'INT le
#    temps de tester une branche non fusionnée) ;
# 2. variable d'environnement en majuscules (COS_TF_BRANCH, COS_TF_LOG_LEVEL) ;
# 3. défaut de l'environnement ci-dessus.
BRANCH_SETTING = "cos_tf_branch"
TF_LOG_SETTING = "cos_tf_log_level"


class EnvSettings(NamedTuple):
    tags: list[str]
    branch: str
    tf_log: str


def _setting(name: str, default: str) -> str:
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


def settings_for(orchestrator_env) -> EnvSettings:
    """Tags, branche Terraform et TF_LOG d'un environnement (enum ou sa valeur).

    Lève une ``ValueError`` explicite sur un environnement inconnu au lieu
    d'envoyer une branche vide à Schematics.
    """
    key = getattr(orchestrator_env, "value", orchestrator_env)
    if key not in DEFAULT_BRANCHES:
        raise ValueError(
            f"Unknown orchestrator environment '{key}', expected one of {sorted(DEFAULT_BRANCHES)}"
        )
    return EnvSettings(
        tags=[ENV_TAGS[key], "agent:ga", "version:1.0"],
        branch=_setting(BRANCH_SETTING, DEFAULT_BRANCHES[key]),
        tf_log=_setting(TF_LOG_SETTING, DEFAULT_TF_LOG[key]),
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
) -> dict:
    """Crée le workspace, ou le met à jour s'il existe déjà sous ce nom."""
    settings = settings_for(orchestrator_env)
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
):
    """Met à jour tags, VCS, description et env du workspace (pas ses variables)."""
    settings = settings_for(orchestrator_env)
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


def run_workspace(tf: SchematicsBackend, workspace_id: str) -> dict:
    """Plan puis apply, et renvoie les outputs Terraform du premier template.

    Lève une ``RuntimeError`` lisible si l'apply n'a produit aucun output, au
    lieu d'un ``IndexError`` anonyme chez l'appelant.
    """
    workspace = tf.workspaces.get_by_id(workspace_id)
    workspace.plan()
    apply_activity = workspace.apply()

    for template_id, template_logs in apply_activity.get_logs().items():
        logger.info("apply logs of workspace %s, template %s:\n%s", workspace_id, template_id, template_logs)

    outputs = workspace.get_outputs()
    if not outputs or not outputs[0].output_values:
        raise RuntimeError(f"workspace {workspace_id} produced no Terraform outputs after apply")
    return dict(outputs[0].output_values[0])
