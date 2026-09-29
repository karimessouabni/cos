# Ressources réelles du provider orchestrator, pilotées par des variables.
# Un scénario (tests/*.tftest.hcl) ne fait varier que `buckets` et
# `with_vault` d'un run à l'autre ; le contexte (environment, realm, apcode,
# tier, prefix) vient de envs/<env>.tfvars. Les assertions lisent
# directement les attributs des ressources.

# --- Instance COS : toujours présente, une par fichier de test ---------------
resource "orchestrator_subscription_cos_v1" "cos" {
  environment = var.environment
  description = "${var.prefix} ${var.scenario} cos"
  apcode      = var.apcode
  realm       = var.realm
  tier        = var.tier
  payload     = {}
}

# --- Backup vault : seulement si le scénario le demande ---------------------
resource "orchestrator_subscription_cosbackup_vault_v1" "vault" {
  count = var.with_vault ? 1 : 0

  environment = var.environment
  description = "${var.prefix} ${var.scenario} vault"
  apcode      = var.apcode
  realm       = var.realm
  payload = {
    cos_instance = orchestrator_subscription_cos_v1.cos.name
  }
}

# --- Buckets : un par clé de var.buckets ------------------------------------
# Le payload est celui de BucketCreatePayload / BucketUpdatePayload (DAGs
# cos.bucket.v1.*) : seules les clés renseignées sont envoyées, pour que
# create et update transportent exactement ce que le scénario demande.
locals {
  bucket_payloads = {
    for key, b in var.buckets : key => merge(
      {
        storage_class = b.storage_class
        cos_instance  = orchestrator_subscription_cos_v1.cos.name
      },
      { for k, v in {
        enable_versioning          = b.enable_versioning
        enable_custom_permissions  = b.enable_custom_permissions
        immutability_choice        = b.immutability_choice
        object_lock_duration_days  = b.object_lock_duration_days
        object_lock_duration_years = b.object_lock_duration_years
      } : k => v if v != null },
      b.retention == null ? {} : {
        retention = { for k, v in b.retention : k => v if v != null }
      },
      b.backup_retention_days == null ? {} : {
        backup = {
          backup_enabled        = true
          backup_vault_sub_id   = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].id)
          backup_retention_days = b.backup_retention_days
        }
      },
    )
  }
}

resource "orchestrator_subscription_cosbucket_v1" "bucket" {
  for_each = var.buckets

  environment = var.environment
  description = "${var.prefix} ${var.scenario} ${each.key}"
  apcode      = var.apcode
  realm       = var.realm
  tier        = var.tier
  payload     = local.bucket_payloads[each.key]

  depends_on = [orchestrator_subscription_cosbackup_vault_v1.vault]
}

output "cos_name" { value = orchestrator_subscription_cos_v1.cos.name }
output "vault_id" { value = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].id) }
output "bucket_names" { value = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => b.name } }
