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
  description = "Section de ibm_endpoints.json lue par le provider IBM (public comme le module bucket) ; private bascule sur private.*.cloud.ibm.com"
  type        = string
  default     = "public"
  validation {
    condition     = contains(["public", "private", "public-and-private"], var.ibm_visibility)
    error_message = "ibm_visibility : public, private ou public-and-private."
  }
}

variable "hub_account_id" {
  description = "Identifiant du compte hub, celui des workspaces Schematics de l'orchestrateur : la zone CBR ne laisse passer que son Schematics."
  type        = string
  validation {
    condition     = can(regex("^[0-9a-f]{32}$", var.hub_account_id))
    error_message = "hub_account_id : identifiant de compte IBM Cloud (32 caractères hexadécimaux)."
  }
}

variable "probe_enabled" {
  description = "Lister le bucket depuis Schematics à chaque plan/apply (probe.tf) : sorties probe_status_code et bucket_empty."
  type        = bool
  default     = false
}

variable "probe_endpoint" {
  description = "URL du bucket pour la sonde (https://<host>/<bucket>). Vide : endpoint privé de la région."
  type        = string
  default     = ""
}

variable "probe_versions" {
  description = "Bucket versionné : la sonde liste les versions et les delete markers."
  type        = bool
  default     = false
}
