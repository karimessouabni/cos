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
