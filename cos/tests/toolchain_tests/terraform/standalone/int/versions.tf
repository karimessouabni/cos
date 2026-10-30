# Version du provider pour int, écrite en dur (pas de envs/<env>.tfvars ici :
# toolchain_env.py laisse donc ce fichier tel quel). Même valeur que
# provider_version dans ../../envs/int.tfvars : à mettre à jour ensemble.
# La source est qualifiée registry.terraform.io/... : sans hôte, OpenTofu
# irait chercher sur registry.opentofu.org.
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    orchestrator = {
      source  = "registry.terraform.io/bp2i/orchestrator"
      version = "2.4.0-rc.2-int"
    }
  }
}
