# Contexte : le même tfvars que les autres roots (envs/ -> ../envs).
variable "environment" { type = string }
variable "realm" { type = string }
variable "provider_version" { type = string }
variable "cos_instance" { type = string }
variable "apcode" {
  type    = string
  default = "AP85135"
}
variable "tier" {
  type    = string
  default = "P"
}
variable "prefix" {
  type    = string
  default = "toolchain"
}

# --- La restauration ------------------------------------------------------------

variable "restore_point_in_time" {
  description = "Point de restauration ISO 8601 (ex. 2026-10-06T10:30:00Z), dans un recovery range disponible."
  type        = string
}

variable "bucket_key" {
  description = "Clé, dans var.buckets du root persistant, du bucket à restaurer."
  type        = string
  default     = "saved"
}

variable "target_bucket" {
  description = "Nom du bucket cible ; vide = le bucket source lui-même (restauration en place)."
  type        = string
  default     = ""
}

variable "recovery_range_id" {
  description = "Recovery range à utiliser ; vide = celui qui contient restore_point_in_time (choisi par le DAG)."
  type        = string
  default     = ""
}
