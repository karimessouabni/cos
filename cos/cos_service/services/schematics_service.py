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

# Surchargeables par l'environnement d'exécution, sans toucher au code :
# - COS_TF_INT_BRANCH : branche Terraform suivie par l'INT (évite de committer
#   un nom de branche de feature). Le défaut reste la branche actuelle.
# - COS_TF_LOG_LEVEL  : niveau TF_LOG envoyé à Schematics (TRACE, DEBUG, INFO,
#   WARN, ERROR). DEBUG est très verbeux, à baisser en prod.
INT_BRANCH = os.environ.get("COS_TF_INT_BRANCH", "feature/update-retention-to-5-years")
TF_LOG_LEVEL = os.environ.get("COS_TF_LOG_LEVEL", "DEBUG")


class EnvSettings(NamedTuple):
    tags: list[str]
    branch: str


_ENV_SETTINGS: dict[str, EnvSettings] = {
    OrchestratorEnvironment.INT.value: EnvSettings(
        tags=["env:int", "agent:ga", "version:1.0"], branch=INT_BRANCH
    ),
    OrchestratorEnvironment.PREPROD.value: EnvSettings(
        tags=["env:pprod", "agent:ga", "version:1.0"], branch="preprod"
    ),
    OrchestratorEnvironment.PROD.value: EnvSettings(
        tags=["env:prod", "agent:ga", "version:1.0"], branch="prod"
    ),
}


def settings_for(orchestrator_env) -> EnvSettings:
    """Tags et branche Terraform d'un environnement (enum ou sa valeur).

    Lève une ``ValueError`` explicite sur un environnement inconnu au lieu
    d'envoyer une branche vide à Schematics.
    """
    key = getattr(orchestrator_env, "value", orchestrator_env)
    try:
        return _ENV_SETTINGS[key]
    except KeyError:
        raise ValueError(
            f"Unknown orchestrator environment '{key}', expected one of {sorted(_ENV_SETTINGS)}"
        ) from None


def _vcs(settings: EnvSettings, tf_directory: str, gitlab_token: str) -> VCS:
    return VCS(
        repository=constants.TERRAFORM_REPOSITORY,
        branch=settings.branch,
        oauth_token_id=gitlab_token,
        directory=tf_directory,
    )


def _env_values() -> list[dict]:
    return [{"TF_LOG": TF_LOG_LEVEL}]


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
        env_values=_env_values(),
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
        env_values=_env_values(),
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
