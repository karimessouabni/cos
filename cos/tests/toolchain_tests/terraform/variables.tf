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
    # --- sauvegarde : le bloc backup est envoyé dès qu'une de ces clés est
    # renseignée ; backup_enabled vaut true par défaut, backup_vault_name le
    # vault du scénario (with_vault) ; les surcharger sert aux cas de refus.
    backup_retention_days = optional(number) # crée le backup vault automatiquement
    backup_enabled        = optional(bool)
    backup_vault_name     = optional(string)
    # --- contexte : instance COS autre que celle du scénario (cas de refus)
    cos_instance = optional(string)
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
  # classes de stockage...) sont celles des DAGs. Les scénarios générés
  # (generate_tests.py) envoient aussi des payloads invalides et vérifient que
  # l'orchestrateur les refuse avec le motif du DAG : c'est le produit qui est
  # testé, pas une copie de ses règles.
}
