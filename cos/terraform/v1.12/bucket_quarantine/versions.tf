terraform {
  required_providers {
    ibm   = { source = "IBM-Cloud/ibm" }
    vault = { source = "hashicorp/vault" }
    http  = { source = "hashicorp/http", version = ">= 3.0" } # sonde du bucket (probe.tf) : un statut non 2xx n'est pas une erreur
  }
}
