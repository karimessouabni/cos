# Contexte commun à tous les scénarios. Seuls `environment`, `realm` et la
# version du provider changent d'un environnement à l'autre : envs/<env>.tfvars.

variable "environment" {
  description = "Environnement orchestrator ciblé : int, qual, pprod, prod."
  type        = string

  validation {
    condition     = contains(["int", "qual", "pprod", "prod"], var.environment)
    error_message = "environment doit valoir int, qual, pprod ou prod."
  }
}

variable "realm" {
  description = "Realm (rlXXXX) dans lequel les souscriptions sont créées."
  type        = string
}

variable "provider_version" {
  description = "Version du provider orchestrator (lue par toolchain_env.py pour écrire versions.tf)."
  type        = string
}

variable "apcode" {
  type    = string
  default = "AP85135"
}

variable "tier" {
  type    = string
  default = "P"
}

variable "prefix" {
  description = "Préfixe des descriptions : identifie le lanceur (user, pipeline) dans l'orchestrateur."
  type        = string
  default     = "toolchain"
}

# --- Ce que chaque scénario fait varier ---------------------------------------

variable "scenario" {
  description = "Nom du scénario, repris dans les descriptions (fixé par chaque fichier de test)."
  type        = string
  default     = "scenario"
}

variable "with_vault" {
  description = "Créer un backup vault sur l'instance COS."
  type        = bool
  default     = false
}

variable "buckets" {
  description = "Buckets à créer sur l'instance COS : clé = nom logique, valeur = options du payload."
  type = map(object({
    storage_class              = optional(string, "standard")
    enable_versioning          = optional(bool)
    enable_custom_permissions  = optional(bool)
    immutability_choice        = optional(string)
    object_lock_duration_days  = optional(number)
    object_lock_duration_years = optional(number)
    backup_retention_days      = optional(number) # nécessite with_vault = true
    retention = optional(object({
      retention_enabled = optional(bool, true)
      default_days      = optional(number)
      minimum_days      = optional(number)
      maximum_days      = optional(number)
      default_years     = optional(number)
      minimum_years     = optional(number)
      maximum_years     = optional(number)
    }))
  }))
  default = {}

  validation {
    condition     = alltrue([for b in values(var.buckets) : contains(["standard", "vault", "cold", "smart"], b.storage_class)])
    error_message = "storage_class : standard, vault, cold ou smart."
  }

  validation {
    condition = alltrue([for b in values(var.buckets) : b.immutability_choice == null ? true : contains([
      "none", "retention", "retention_daily", "retention_yearly",
      "object_lock", "object_lock_daily", "object_lock_yearly",
    ], b.immutability_choice)])
    error_message = "immutability_choice invalide."
  }

  # `||` n'est pas paresseux en HCL : les conditions sont écrites avec `? :`
  # pour ne jamais indexer une rétention nulle.
  validation {
    condition = alltrue([for b in values(var.buckets) : b.retention == null ? true : !(
      anytrue([for k in ["default_days", "minimum_days", "maximum_days"] : lookup(b.retention, k, null) != null]) &&
      anytrue([for k in ["default_years", "minimum_years", "maximum_years"] : lookup(b.retention, k, null) != null])
    )])
    error_message = "retention : ne pas mélanger les bornes en jours et en années (ADR 0001)."
  }

  validation {
    condition = alltrue(flatten([for b in values(var.buckets) : b.retention == null ? [true] : [
      for unit in ["days", "years"] : (
        lookup(b.retention, "minimum_${unit}", null) == null ||
        lookup(b.retention, "default_${unit}", null) == null ||
        lookup(b.retention, "maximum_${unit}", null) == null
        ) ? true : (
        b.retention["minimum_${unit}"] <= b.retention["default_${unit}"] &&
        b.retention["default_${unit}"] <= b.retention["maximum_${unit}"]
      )
    ]]))
    error_message = "retention : minimum <= default <= maximum."
  }

  validation {
    condition = alltrue([for b in values(var.buckets) : b.retention == null ? true : (
      coalesce(lookup(b.retention, "maximum_years", null), 0) <= 5 &&
      coalesce(lookup(b.retention, "maximum_days", null), 0) <= 1827
    )])
    error_message = "retention : plafond 5 ans (1827 jours)."
  }
}
