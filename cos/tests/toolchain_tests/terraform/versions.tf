# Généré par toolchain_env.py depuis envs/<env>.tfvars (provider_version) :
# une contrainte de version ne peut pas être une variable Terraform, et la
# version du provider orchestrator diffère par environnement (2.3.0-int, ...).
# Ne pas éditer à la main : `python ../toolchain_env.py --env <env>` le réécrit.
terraform {
  required_version = ">= 1.6.0" # terraform test

  required_providers {
    orchestrator = {
      source  = "bp2i/orchestrator"
      version = "2.3.0-int"
    }
  }
}
