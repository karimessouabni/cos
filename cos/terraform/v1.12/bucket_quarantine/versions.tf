terraform {
  required_version = ">= 1.5.0" # Schematics exécute la version 1.12 (TERRAFORM_VERSION)

  required_providers {
    ibm   = { source = "IBM-Cloud/ibm" }
    vault = { source = "hashicorp/vault" }
  }
}
