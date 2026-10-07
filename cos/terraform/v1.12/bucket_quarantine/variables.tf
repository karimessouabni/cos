variable "bucket_name" {
  description = "Nom du bucket en quarantaine (depuis la base, jamais lu sur COS)."
  type        = string
}

variable "cos_instance_crn" {
  description = "CRN de l'instance COS du bucket."
  type        = string
}

variable "allowed_vpc_crns" {
  description = "VPC de l'orchestrateur (Airflow / VPE) autorisés pendant la quarantaine. Aucun VPC client ne doit y figurer."
  type        = list(string)
  default     = []
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
  type = string
}

variable "orchestrator_environment" {
  type = string
}

variable "region" {
  type = string
}
