# Aligné sur terraform/v1.12/bucket/providers.tf : même lecture de la clé API du
# compte workload dans Vault (sortie `secrets` du module de lecture), même
# provider ibm. Le fichier `ibm_endpoints.json` du module bucket est pris s'il
# est copié ici ; sans lui, le provider plante au plan ("Unable to open
# Endpoints File"), d'où le test d'existence. La règle CBR ne parle qu'à l'API
# CBR, jointe par son endpoint par défaut si le fichier ne la liste pas.
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
  ibmcloud_api_key    = module.vault.secrets["api_key"]
  region              = var.region
  # Depuis Schematics, cbr.cloud.ibm.com ne répond pas (INT : "context deadline
  # exceeded") : la route vers CBR est donnée par ibm_endpoints.json, clé
  # IBMCLOUD_CONTEXT_BASED_RESTRICTIONS_ENDPOINT, comme IAM et COS pour le bucket.
  visibility          = var.ibm_visibility
  endpoints_file_path = fileexists("${path.module}/ibm_endpoints.json") ? "${path.module}/ibm_endpoints.json" : null
}
