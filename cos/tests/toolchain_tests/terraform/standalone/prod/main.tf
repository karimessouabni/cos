# Root AUTONOME pour prod : une instance COS, un backup vault sur cette
# instance, et un bucket versionné sauvegardé dans ce vault. Aucune variable :
# tout le contexte est écrit en dur ci-dessous (valeurs reprises de
# ../../envs/prod.tfvars), pour pouvoir lancer un apply en une commande.
#
# Depuis cos/tests/toolchain_tests (clé API, proxy, login, init gérés) :
#   python toolchain_env.py --env prod --dir terraform/standalone/prod --run apply
#   python toolchain_env.py --env prod --dir terraform/standalone/prod --run plan
#   python toolchain_env.py --env prod --dir terraform/standalone/prod --run destroy
# Ou à la main, dans ce dossier, avec ORCHESTRATOR_IBMCLOUD_API_KEY exportée :
#   tofu init && tofu apply
#
# Le state est local (terraform.tfstate, ignoré par git) : le destroy passe
# par le même dossier. L'ordre est imposé par les références : COS -> vault
# -> bucket (backup_vault_name = nom du vault, le DAG résout le sub_id).
# Pour réutiliser une instance COS existante au lieu d'en créer une :
# supprimer la ressource "cos" et remplacer local.cos_name par son nom.

locals {
  environment = "prod"
  realm       = "rl002i000102" # à vérifier (repris de envs/prod.tfvars)
  apcode      = "AP85135"
  tier        = "P"
  prefix      = "standalone prod" # repris dans les descriptions des souscriptions
}

# --- Instance COS -------------------------------------------------------------
resource "orchestrator_subscription_cos_v1" "cos" {
  environment = local.environment
  description = "${local.prefix} cos"
  apcode      = local.apcode
  realm       = local.realm
  tier        = local.tier
  payload     = {}
}

locals {
  cos_name = orchestrator_subscription_cos_v1.cos.name
}

# --- Backup vault sur cette instance ------------------------------------------
resource "orchestrator_subscription_cosbackup_vault_v1" "vault" {
  environment = local.environment
  description = "${local.prefix} vault"
  apcode      = local.apcode
  realm       = local.realm
  payload = {
    cos_instance = local.cos_name
  }
}

# --- Bucket sauvegardé dans le vault ------------------------------------------
# Payload BucketCreatePayload (DAG cos.bucket.v1.create). Le versioning est
# requis pour la sauvegarde (scénario tests/50_backup.tftest.hcl).
resource "orchestrator_subscription_cosbucket_v1" "bucket" {
  environment = local.environment
  description = "${local.prefix} bucket"
  apcode      = local.apcode
  realm       = local.realm
  tier        = local.tier
  payload = {
    storage_class     = "standard"
    cos_instance      = local.cos_name
    enable_versioning = true
    backup = {
      backup_enabled        = true
      backup_vault_name     = orchestrator_subscription_cosbackup_vault_v1.vault.name
      backup_retention_days = 7
    }
  }
}

output "cos_name" { value = local.cos_name }
output "vault_name" { value = orchestrator_subscription_cosbackup_vault_v1.vault.name }
output "vault_id" { value = orchestrator_subscription_cosbackup_vault_v1.vault.id }
output "bucket_name" { value = orchestrator_subscription_cosbucket_v1.bucket.name }
output "bucket_id" { value = orchestrator_subscription_cosbucket_v1.bucket.id }
