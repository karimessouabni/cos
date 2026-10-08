# Aligné sur terraform/v1.12/bucket/providers.tf : même lecture de la clé API du
# compte workload dans Vault (sortie `secrets` du module de lecture), même
# provider ibm. Le fichier `ibm_endpoints.json` du module bucket n'est pas
# repris : la règle CBR ne parle qu'à IAM et à l'API CBR, par leurs endpoints
# par défaut. S'il s'avère nécessaire depuis Schematics, le copier ici et
# ajouter `endpoints_file_path = "ibm_endpoints.json"` au provider ibm.
terraform {
  required_providers {
    ibm   = { source = "IBM-Cloud/ibm" }
    vault = { source = "hashicorp/vault" }
  }
}

provider "vault" {
  address          = var.vault_read_addr
  token            = var.vault_read_token
  skip_child_token = true
  alias            = "read"
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
  ibmcloud_api_key = module.vault.secrets["api_key"]
  region           = var.region
}
