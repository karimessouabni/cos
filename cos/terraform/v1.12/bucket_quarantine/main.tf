################################################################################
# Quarantaine d'un bucket pendant la période de grâce d'un clean
# (docs/adr/0003-periode-de-grace-du-clean.md, v2)
################################################################################
# Workspace séparé du bucket : son state ne contient que la zone et la règle
# CBR. Son refresh n'appelle que l'API CBR, qu'une règle CBR sur COS ne bloque
# jamais : il reste pilotable quoi que la règle bloque, et la lever est toujours
# possible. Le bucket n'est jamais lu ici, son nom et son instance arrivent en
# variables depuis la base de l'orchestrateur.
#
# La règle n'autorise que les endpoints direct / private depuis la zone réseau
# de l'orchestrateur (VPC de la plateforme, référence de service Schematics).
# Les clients, sur l'URL publique, reçoivent 403 quel que soit leur proxy.

data "ibm_iam_account_settings" "quarantine" {}

locals {
  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)
}

resource "ibm_cbr_zone" "quarantine" {
  name        = "quarantine-${var.bucket_name}"
  description = "Orchestrateur seulement, le temps de la période de grâce du clean"

  dynamic "addresses" {
    for_each = var.allowed_vpc_crns
    content {
      type  = "vpc"
      value = addresses.value
    }
  }

  # Schematics doit rester autorisé (applies du bucket après la quarantaine).
  addresses {
    type = "serviceRef"
    ref {
      service_name = "schematics"
    }
  }
}

resource "ibm_cbr_rule" "quarantine" {
  description      = "Quarantaine du bucket ${var.bucket_name} (clean programmé)"
  enforcement_mode = var.enforcement_mode

  dynamic "contexts" {
    for_each = toset(["direct", "private"])
    content {
      attributes {
        name  = "networkZoneId"
        value = ibm_cbr_zone.quarantine.id
      }
      attributes {
        name  = "endpointType"
        value = contexts.value
      }
    }
  }

  resources {
    attributes {
      name  = "accountId"
      value = data.ibm_iam_account_settings.quarantine.account_id
    }
    attributes {
      name  = "serviceName"
      value = "cloud-object-storage"
    }
    attributes {
      name  = "serviceInstance"
      value = local.cos_instance_guid
    }
    attributes {
      name  = "resourceType"
      value = "bucket"
    }
    attributes {
      name  = "resource"
      value = var.bucket_name
    }
  }
}

output "cbr_rule_id" {
  description = "Identifiant de la règle : sortie de secours `ibmcloud cbr rule-delete <id>`."
  value       = ibm_cbr_rule.quarantine.id
}

output "cbr_zone_id" {
  value = ibm_cbr_zone.quarantine.id
}
