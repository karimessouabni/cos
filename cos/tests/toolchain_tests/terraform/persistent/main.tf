# Environnement PERSISTANT : un backup vault et un bucket sauvegardé qui ne sont
# jamais détruits par un test. Contrairement à ../tests/*.tftest.hcl (state en
# mémoire, destruction automatique), ce root a un vrai state local
# (terraform.tfstate, ignoré par git) :
#
#   python ../toolchain_env.py --env int --dir terraform/persistent --run apply    # créer / mettre à jour
#   python ../toolchain_env.py --env int --dir terraform/persistent --run plan     # vérifier l'absence de drift
#   python ../toolchain_env.py --env int --dir terraform/persistent --run destroy  # seulement quand on a fini
#
# Flux prévu : apply -> déposer des objets dans le bucket -> attendre une
# sauvegarde -> ../restore (restauration à un point dans le temps).
# L'instance COS est toujours celle de cos_instance (envs/<env>.tfvars).

# --- Backup vault : toujours présent, c'est lui qui sauvegarde les buckets ----
resource "orchestrator_subscription_cosbackup_vault_v1" "vault" {
  count = 1 # toujours : c'est lui qu'on garde pour les restaurations

  environment = var.environment
  description = "${var.prefix} ${var.scenario} vault"
  apcode      = var.apcode
  realm       = var.realm
  payload = {
    cos_instance = var.cos_instance
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
        cos_instance  = var.cos_instance
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
          backup_vault_name     = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].name) # le DAG résout le sub_id
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
  # Pas de depends_on : un bucket ne dépend du vault que si son payload le
  # référence (backup_vault_name), donc seulement avec backup_retention_days.
}

output "vault_id" { value = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].id) }
output "bucket_names" { value = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => b.name } }
output "vault_name" { value = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].name) }

# Issue de la demande vue par le provider : status (ACTIVE, DECLINED, ...) et
# motif. Les scénarios d'échec (60_*) assertent dessus : une demande refusée
# par le DAG doit se voir ici, avec le message du DAG. Les noms d'attributs
# sont ceux de la console orchestrator ; try() tolère un provider qui les
# nomme autrement (les assertions diront alors "null").
output "bucket_status" {
  value = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => try(b.status, b.state.status, null) }
}
output "bucket_status_reason" {
  value = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => try(b.status_reason, b.state.status_reason, "") }
}
output "bucket_ids" { value = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => b.id } }
