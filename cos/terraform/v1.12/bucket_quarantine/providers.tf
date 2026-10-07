# [À ALIGNER] sur terraform/v1.12/bucket/providers.tf (absent du dépôt) : même
# lecture de la clé API du compte workload dans Vault, même provider ibm.
terraform {
  required_providers {
    ibm   = { source = "IBM-Cloud/ibm" }
    vault = { source = "hashicorp/vault" }
  }
}

provider "vault" {
  alias   = "read"
  address = var.vault_read_addr
  token   = var.vault_read_token
}

module "vault" {
  source      = "git::https://gitlab-dogen.group.echonet/market-place/ap43584/orchestrator/terraform/modules/terraform-module-vault.git//read?ref=v2.0.0"
  secret_path = "${local.vault_namespace}/${local.vault_secret_path_account}"
  providers = {
    vault = vault.read
  }
}

locals {
  # Identiques au workspace du bucket.
  vault_namespace           = var.app_code
  vault_secret_path_account = "ibm/${var.wklapp_account_id}"
}

provider "ibm" {
  ibmcloud_api_key = module.vault.secret["apikey"]
  region           = var.region
}
