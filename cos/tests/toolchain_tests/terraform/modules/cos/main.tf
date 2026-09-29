# Instance COS : souscription orchestrator_subscription_cos_v1.
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

resource "orchestrator_subscription_cos_v1" "this" {
  environment = var.environment
  description = var.description
  apcode      = var.apcode
  realm       = var.realm
  tier        = var.tier
  payload     = {}
}

output "id" { value = orchestrator_subscription_cos_v1.this.id }
output "name" { value = orchestrator_subscription_cos_v1.this.name }
