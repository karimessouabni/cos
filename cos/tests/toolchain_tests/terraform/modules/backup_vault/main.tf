# Backup vault rattaché à une instance COS : orchestrator_subscription_cosbackup_vault_v1.
terraform {
  required_providers {
    orchestrator = { source = "bp2i/orchestrator" }
  }
}

variable "environment" { type = string }
variable "realm" { type = string }
variable "apcode" { type = string }
variable "description" { type = string }
variable "cos_instance" {
  description = "Nom (output `name`) de la souscription COS."
  type        = string
}

resource "orchestrator_subscription_cosbackup_vault_v1" "this" {
  environment = var.environment
  description = var.description
  apcode      = var.apcode
  realm       = var.realm
  payload = {
    cos_instance = var.cos_instance
  }
}

output "id" { value = orchestrator_subscription_cosbackup_vault_v1.this.id }
output "name" { value = orchestrator_subscription_cosbackup_vault_v1.this.name }
