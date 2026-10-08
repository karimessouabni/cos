variable "bucket_name" {
  description = "Nom du bucket en quarantaine (depuis la base, jamais lu sur COS)."
  type        = string
}

variable "cos_instance_crn" {
  description = "CRN de l'instance COS du bucket."
  type        = string
}

variable "enforcement_mode" {
  description = "enabled : bloque ; report : journalise seulement (CBR), pour valider la zone sur les premiers clients."
  type        = string
  default     = "enabled"
  validation {
    condition     = contains(["enabled", "report", "disabled"], var.enforcement_mode)
    error_message = "enforcement_mode doit valoir enabled, report ou disabled."
  }
}

# Mêmes variables d'authentification que le workspace du bucket : la clé API
# IBM Cloud du compte workload est lue dans Vault (voir providers.tf).
variable "vault_read_addr" {
  type = string
}

variable "vault_read_token" {
  type      = string
  sensitive = true
}

variable "app_code" {
  type = string
}

variable "wklapp_account_id" {
  description = "Identifiant du compte workload (realm.wklapp_account_number) : chemin Vault de la clé API et compte des ressources CBR"
  type        = string
}

variable "orchestrator_environment" {
  type = string
}

variable "region" {
  type = string
}

variable "ibm_visibility" {
  description = "Endpoints du provider IBM : private (private.cbr.cloud.ibm.com), public, ou public-and-private"
  type        = string
  default     = "private"
  validation {
    condition     = contains(["public", "private", "public-and-private"], var.ibm_visibility)
    error_message = "ibm_visibility : public, private ou public-and-private."
  }
}
