################################################################################
# GET APIKEY FROM VAULT WKLAPP
################################################################################
# [RECONSTITUTION] Reconstitué depuis la capture PyCharm (31 lignes, toutes visibles).
module "vault" {
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//read?ref=v2.0.0"
  secret_path = "${local.vault_namespace}/${local.vault_secret_path_account}"
  providers = {
    vault = vault.read
  }
}

################################################################################
# Create backup vault in the COS instance
################################################################################

resource "random_id" "vault_suffix" {
  byte_length = 4
}

module "backup_vault" {
  source                            = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-cos.git//modules/backup_vault?ref=10.14.8"
  name                              = local.vault_name
  existing_cos_instance_id          = var.cos_instance_id
  region                            = var.region
  kms_encryption_enabled            = true
  kms_key_crn                       = var.kms_key_crn
  skip_kms_iam_authorization_policy = true
}
