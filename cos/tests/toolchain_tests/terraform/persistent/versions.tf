# Généré par toolchain_env.py depuis envs/<env>.tfvars (provider_version) :
# une contrainte de version ne peut pas être une variable, et la version du
# provider orchestrator diffère par environnement (2.3.0-int, ...).
# La source est qualifiée registry.terraform.io/... : sans hôte, OpenTofu
# irait chercher sur registry.opentofu.org.
# Ne pas éditer à la main : `python ../toolchain_env.py --env <env>` le réécrit.
terraform {
  required_version = ">= 1.6.0" # tofu test

  required_providers {
    orchestrator = {
      source  = "registry.terraform.io/bp2i/orchestrator"
      version = "2.3.0-int"
    }
  }
}
