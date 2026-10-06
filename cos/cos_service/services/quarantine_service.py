"""Quarantaine d'un bucket pendant la période de grâce d'un clean.

Une règle Context-Based Restrictions (CBR) sur le bucket n'autorise plus que
les endpoints ``direct`` / ``private`` et la zone réseau de l'orchestrateur :
les clients, qui passent par l'URL publique (clé API ou HMAC, avec ou sans
proxy), reçoivent 403 ; Airflow (endpoint direct) et Schematics (référence de
service) continuent de passer.

La règle est portée par le Terraform du workspace du bucket
(``terraform/v1.12/bucket/quarantine.tf``, variable ``quarantine``) : elle vit
dans le compte workload avec les accès croisés que Schematics a déjà, elle est
dans le state Terraform donc jamais orpheline, et la lever est le même apply
avec ``quarantine = false``.

Contrat attendu de ``workspaceService.build_bucket_workspace_details`` : un
argument nommé ``quarantine`` (bool) reporté dans la variable Terraform du
même nom. Voir docs/adr/0003-periode-de-grace-du-clean.md.
"""
import logging

logger = logging.getLogger(__name__)


def _workspace_details(bucket: dict, description: str, quarantine: bool, session) -> dict:
    from cos_service.services.immutability_service import compute_bucket_immutability_for_update_bucket
    from cos_service.services.workspaceService import build_bucket_workspace_details

    backup_vault = bucket.get("backup_vault")
    return build_bucket_workspace_details(
        workspace_id=bucket["workspace"]["workspace_id"],
        realm=bucket["cos"]["context"]["realm"],
        app_code=bucket["cos"]["context"]["app_code"],
        region=bucket["region"],
        storage_class=bucket["storage_class"],
        cos_instance_crn=bucket["cos"]["crn"],
        cos_instance_name=bucket["cos"]["name"],
        activity_tracker_crn=bucket["activity_tracker_crn"],
        kms_crn=bucket["kms_crn"],
        monitoring_crn=bucket["monitoring_crn"],
        management_endpoint_type="direct",
        immutability=compute_bucket_immutability_for_update_bucket(bucket, session),
        enable_custom_permissions=bucket["enable_custom_permissions"],
        description=description,
        backup_vault_crn=backup_vault["crn"] if backup_vault is not None else None,
        quarantine=quarantine,
    )


def set_bucket_quarantine(
    *, bucket: dict, enabled: bool, payload, description: str, tf, vault, reader, session
) -> dict:
    """Pose (``enabled=True``) ou lève la quarantaine par un apply du workspace
    du bucket. Renvoie les outputs Terraform. Lève si l'apply échoue."""
    from cos_service.services.schematics_service import run_workspace
    from cos_service.services.workspaceService import update_bucket_workspace

    workspace_id = bucket["workspace"]["workspace_id"]
    details = _workspace_details(bucket, description, enabled, session)
    logger.info("%s quarantine on bucket %s (workspace %s)",
                "setting" if enabled else "lifting", bucket["name"], workspace_id)
    update_bucket_workspace(payload=payload, workspace_details=details, vault=vault, tf=tf, reader=reader)
    return run_workspace(tf, workspace_id)
