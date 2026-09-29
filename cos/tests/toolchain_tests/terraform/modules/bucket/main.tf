# Bucket COS : orchestrator_subscription_cosbucket_v1.
# Le payload est celui de BucketCreatePayload / BucketUpdatePayload (DAGs
# cos.bucket.v1.*) : seules les clés renseignées sont envoyées, pour que
# create et update transportent exactement ce que le scénario demande.
terraform {
  required_providers {
    orchestrator = { source = "bp2i/orchestrator" }
  }
}

variable "environment" { type = string }
variable "realm" { type = string }
variable "apcode" { type = string }
variable "tier" { type = string }
variable "description" { type = string }
variable "cos_instance" {
  description = "Nom (output `name`) de la souscription COS."
  type        = string
}

variable "storage_class" {
  type    = string
  default = "standard"

  validation {
    condition     = contains(["standard", "vault", "cold", "smart"], var.storage_class)
    error_message = "storage_class : standard, vault, cold ou smart."
  }
}

variable "enable_versioning" {
  type    = bool
  default = null
}

variable "enable_custom_permissions" {
  type    = bool
  default = null
}

variable "immutability_choice" {
  description = "none, retention_daily, retention_yearly, object_lock_daily, object_lock_yearly (retention / object_lock : format historique)."
  type        = string
  default     = null

  validation {
    condition = var.immutability_choice == null ? true : contains([
      "none", "retention", "retention_daily", "retention_yearly",
      "object_lock", "object_lock_daily", "object_lock_yearly",
    ], var.immutability_choice)
    error_message = "immutability_choice invalide."
  }
}

variable "object_lock_duration_days" {
  type    = number
  default = null
}

variable "object_lock_duration_years" {
  type    = number
  default = null
}

variable "retention" {
  description = "Rétention (BucketRetention) : bornes en jours OU en années, jamais les deux."
  type = object({
    retention_enabled = optional(bool, true)
    default_days      = optional(number)
    minimum_days      = optional(number)
    maximum_days      = optional(number)
    default_years     = optional(number)
    minimum_years     = optional(number)
    maximum_years     = optional(number)
  })
  default = null

  # `||` n'est pas paresseux en HCL : les conditions sont écrites avec `? :`
  # pour ne jamais indexer une rétention nulle.
  validation {
    condition = var.retention == null ? true : !(
      anytrue([for k in ["default_days", "minimum_days", "maximum_days"] : lookup(var.retention, k, null) != null]) &&
      anytrue([for k in ["default_years", "minimum_years", "maximum_years"] : lookup(var.retention, k, null) != null])
    )
    error_message = "retention : ne pas mélanger les bornes en jours et en années (ADR 0001)."
  }

  validation {
    condition = var.retention == null ? true : alltrue([
      for unit in ["days", "years"] : (
        lookup(var.retention, "minimum_${unit}", null) == null ||
        lookup(var.retention, "default_${unit}", null) == null ||
        lookup(var.retention, "maximum_${unit}", null) == null
        ) ? true : (
        var.retention["minimum_${unit}"] <= var.retention["default_${unit}"] &&
        var.retention["default_${unit}"] <= var.retention["maximum_${unit}"]
      )
    ])
    error_message = "retention : minimum <= default <= maximum."
  }

  validation {
    condition = var.retention == null ? true : (
      coalesce(lookup(var.retention, "maximum_years", null), 0) <= 5 &&
      coalesce(lookup(var.retention, "maximum_days", null), 0) <= 1827
    )
    error_message = "retention : plafond 5 ans (1827 jours)."
  }
}

variable "backup" {
  description = "Sauvegarde : id de la souscription backup vault et rétention des sauvegardes en jours."
  type = object({
    vault_sub_id   = string
    retention_days = number
  })
  default = null
}

locals {
  retention_payload = var.retention == null ? {} : {
    retention = { for k, v in var.retention : k => v if v != null }
  }
  backup_payload = var.backup == null ? {} : {
    backup = {
      backup_enabled        = true
      backup_vault_sub_id   = var.backup.vault_sub_id
      backup_retention_days = var.backup.retention_days
    }
  }
  optional_payload = { for k, v in {
    enable_versioning          = var.enable_versioning
    enable_custom_permissions  = var.enable_custom_permissions
    immutability_choice        = var.immutability_choice
    object_lock_duration_days  = var.object_lock_duration_days
    object_lock_duration_years = var.object_lock_duration_years
  } : k => v if v != null }
}

resource "orchestrator_subscription_cosbucket_v1" "this" {
  environment = var.environment
  description = var.description
  apcode      = var.apcode
  realm       = var.realm
  tier        = var.tier
  payload = merge(
    {
      storage_class = var.storage_class
      cos_instance  = var.cos_instance
    },
    local.optional_payload,
    local.retention_payload,
    local.backup_payload,
  )
}

output "id" { value = orchestrator_subscription_cosbucket_v1.this.id }
output "name" { value = orchestrator_subscription_cosbucket_v1.this.name }
output "payload" { value = orchestrator_subscription_cosbucket_v1.this.payload }
