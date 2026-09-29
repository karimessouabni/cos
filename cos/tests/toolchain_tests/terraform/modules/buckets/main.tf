# Plusieurs buckets d'un coup sur la même instance COS (for_each sur une map).
# Dans terraform test, deux run sur le même module partagent un seul state :
# un second run "crée" en réalité une mise à jour. Pour tester plusieurs
# buckets indépendants dans un même fichier, on passe donc par ce module.
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
variable "cos_instance" { type = string }

variable "buckets" {
  description = "Clé = nom logique du bucket ; valeur = les options du module bucket."
  type = map(object({
    storage_class              = optional(string, "standard")
    enable_versioning          = optional(bool)
    enable_custom_permissions  = optional(bool)
    immutability_choice        = optional(string)
    object_lock_duration_days  = optional(number)
    object_lock_duration_years = optional(number)
    retention = optional(object({
      retention_enabled = optional(bool, true)
      default_days      = optional(number)
      minimum_days      = optional(number)
      maximum_days      = optional(number)
      default_years     = optional(number)
      minimum_years     = optional(number)
      maximum_years     = optional(number)
    }))
    backup = optional(object({
      vault_sub_id   = string
      retention_days = number
    }))
  }))
}

module "bucket" {
  source   = "../bucket"
  for_each = var.buckets

  environment                = var.environment
  realm                      = var.realm
  apcode                     = var.apcode
  tier                       = var.tier
  description                = "${var.description} ${each.key}"
  cos_instance               = var.cos_instance
  storage_class              = each.value.storage_class
  enable_versioning          = each.value.enable_versioning
  enable_custom_permissions  = each.value.enable_custom_permissions
  immutability_choice        = each.value.immutability_choice
  object_lock_duration_days  = each.value.object_lock_duration_days
  object_lock_duration_years = each.value.object_lock_duration_years
  retention                  = each.value.retention
  backup                     = each.value.backup
}

output "ids" { value = { for k, m in module.bucket : k => m.id } }
output "names" { value = { for k, m in module.bucket : k => m.name } }
output "payloads" { value = { for k, m in module.bucket : k => m.payload } }
