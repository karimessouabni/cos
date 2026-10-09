variable "bucket_name" {
  description = "Nom du bucket en quarantaine (depuis la base, jamais lu sur COS)."
  type        = string
}

variable "cos_instance_crn" {
  description = "CRN de l'instance COS du bucket."
  type        = string
}

variable "enforcement_mode" {
  description = "Mode de la règle quand elle est active : enabled bloque ; report journalise seulement (CBR), pour valider sur les premiers clients."
  type        = string
  default     = "enabled"
  validation {
    condition     = contains(["enabled", "report"], var.enforcement_mode)
    error_message = "enforcement_mode doit valoir enabled ou report (la désactivation passe par rule_active)."
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
  description = "Compte workload tel que le module bucket le reçoit (realm.wklapp_account_number) : chemin Vault de la clé API."
  type        = string
}

variable "cbr_account_id" {
  description = "Identifiant IBM (32 hexadécimaux) du compte workload, propriétaire de la zone et de la règle CBR (realm.wklapp_account_id)."
  type        = string
}

variable "orchestrator_environment" {
  type = string
}

variable "region" {
  type = string
}

variable "ibm_visibility" {
  description = "Section de ibm_endpoints.json lue par le provider IBM (public comme le module bucket) ; private bascule sur private.*.cloud.ibm.com"
  type        = string
  default     = "public"
  validation {
    condition     = contains(["public", "private", "public-and-private"], var.ibm_visibility)
    error_message = "ibm_visibility : public, private ou public-and-private."
  }
}

variable "rule_active" {
  description = "true : la règle bloque (bucket fermé) ; false : la règle est désactivée le temps d'une action de l'orchestrateur (poser la règle de vidage, vérifier le vidage, supprimer le bucket)."
  type        = bool
  default     = true
}
