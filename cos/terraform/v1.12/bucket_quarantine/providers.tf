# Aligné sur terraform/v1.12/bucket/providers.tf : même lecture de la clé API du
# compte workload dans Vault (sortie `secrets` du module de lecture), même
# provider ibm. Le fichier `ibm_endpoints.json` du module bucket est pris s'il
# est copié ici (il doit router IBMCLOUD_CONTEXT_BASED_RESTRICTIONS_ENDPOINT) ;
# sans lui, le provider plante au plan ("Unable to open Endpoints File"), d'où
# le test d'existence.
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

provider "ibm" {
  ibmcloud_api_key    = module.vault.secrets["api_key"]
  region              = var.region
  visibility          = var.ibm_visibility
  endpoints_file_path = fileexists("${path.module}/ibm_endpoints.json") ? "${path.module}/ibm_endpoints.json" : null
}
