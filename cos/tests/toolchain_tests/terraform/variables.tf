# Contexte commun à tous les scénarios. Seuls `environment`, `realm` et la
# version du provider changent d'un environnement à l'autre : envs/<env>.tfvars.

variable "environment" {
  description = "Environnement orchestrator ciblé : int, qual, pprod, prod."
  type        = string

  validation {
    condition     = contains(["int", "qual", "qua", "pprod", "prod"], var.environment)
    error_message = "environment doit valoir int, qual, qua, pprod ou prod."
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

variable "cos_instance" {
  description = "Nom d'une instance COS existante réutilisée par tous les scénarios ; vide = créée (et détruite) par le scénario."
  type        = string
  default     = ""
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
  description = "Créer un backup vault sur l'instance COS même sans bucket sauvegardé (sinon il est créé automatiquement dès qu'un bucket a backup_retention_days)."
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
    # sauvegarde : backup_retention_days crée le backup vault automatiquement ;
    # backup_enabled = false (avec versioning) désactive la sauvegarde d'un bucket.
    backup_retention_days = optional(number)
    backup_enabled        = optional(bool)
    retention = optional(object({
      retention_enabled = optional(bool, true)
      # format historique (ADR 0001) : jours implicites, déprécié mais accepté
      default = optional(number)
      minimum = optional(number)
      maximum = optional(number)
      # format courant : une seule unité par demande
      default_days  = optional(number)
      minimum_days  = optional(number)
      maximum_days  = optional(number)
      default_years = optional(number)
      minimum_years = optional(number)
      maximum_years = optional(number)
    }))
  }))
  default = {}
  # Pas de validation ici : les règles métier (unités de rétention, bornes,
  # classes de stockage...) sont celles des DAGs, testées dans tests/unit/.
}
