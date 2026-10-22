# Contrainte de version exigée par tflint (terraform_required_version).
# Schematics exécute la version déclarée par TERRAFORM_VERSION (1.12) de
# cos_service/services/schematics_service.py.
terraform {
  required_version = ">= 1.5.0"
}
