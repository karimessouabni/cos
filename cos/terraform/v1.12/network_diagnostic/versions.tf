terraform {
  required_version = ">= 1.5.0" # Schematics exécute la version 1.12 (TERRAFORM_VERSION)

  required_providers {
    dns = { source = "hashicorp/dns", version = ">= 3.0" }
  }
}
