# Restauration d'un bucket de l'environnement persistant (../persistent) à un
# point dans le temps : déclenche le DAG cos.bucket.v1.restore sur la
# souscription du bucket, avec le backup vault qui le sauvegarde.
#
#   python ../toolchain_env.py --env int --dir terraform/restore --run apply -- -var restore_point_in_time=2026-10-06T10:30:00Z
#
# Le bucket et le vault sont lus dans le state de ../persistent : rien n'est
# créé ni détruit ici, seule la demande de restauration est envoyée.

data "terraform_remote_state" "persistent" {
  backend = "local"
  config = {
    path = "${path.module}/../persistent/terraform.tfstate"
  }
}

locals {
  bucket_name = data.terraform_remote_state.persistent.outputs.bucket_names[var.bucket_key]
  bucket_id   = data.terraform_remote_state.persistent.outputs.bucket_ids[var.bucket_key]
  vault_name  = data.terraform_remote_state.persistent.outputs.vault_name

  # Payload de BucketRestoreBackupVaultPayload (cos.bucket.v1.restore)
  restore_payload = merge(
    {
      backup_vault_name     = local.vault_name
      target_bucket         = var.target_bucket != "" ? var.target_bucket : local.bucket_name
      app_code              = var.apcode
      realm                 = var.realm
      restore_point_in_time = var.restore_point_in_time
    },
    var.recovery_range_id == "" ? {} : { recovery_range_id = var.recovery_range_id },
  )
}

# ---------------------------------------------------------------------------
# À CONFIRMER dans la documentation du provider orchestrator
# (https://dmzr-docs.group.echonet/terraform/) : le TYPE de ressource qui
# déclenche une action (ici `restore`) sur une souscription existante, et le
# nom de l'attribut qui porte l'id de la souscription. Les souscriptions sont
# `orchestrator_subscription_<produit>_v1` ; une action a vraisemblablement
# sa propre ressource. Tant que ce n'est pas confirmé, `tofu validate` échoue
# ici volontairement (type inconnu) et rien n'est envoyé.
# ---------------------------------------------------------------------------
resource "orchestrator_action_cosbucket_restore_v1" "restore" {
  environment     = var.environment
  subscription_id = local.bucket_id
  description     = "${var.prefix} restore ${var.bucket_key} @ ${var.restore_point_in_time}"
  payload         = local.restore_payload
}

output "restore_payload" { value = local.restore_payload }
output "bucket_name" { value = local.bucket_name }
output "vault_name" { value = local.vault_name }
