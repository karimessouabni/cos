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
# La règle bloque tout : son seul contexte est une zone réseau qui ne
# correspond à rien (une adresse de documentation, RFC 5737, jamais routée).
# Personne n'a besoin du bucket pendant la grâce, ni le client ni
# l'orchestrateur : la quarantaine est levée à la fin de la grâce, juste avant
# le vidage, qui se fait donc bucket ouvert.

data "ibm_iam_account_settings" "quarantine" {}

locals {
  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)
}

resource "ibm_cbr_zone" "quarantine" {
  name        = "quarantine-${var.bucket_name}"
  description = "Zone vide (adresse de documentation) : aucune requête ne la satisfait"
  # Compte de la clé API lue dans Vault, donc le compte workload qui possède le bucket.
  account_id  = data.ibm_iam_account_settings.quarantine.account_id

  addresses {
    type  = "ipAddress"
    value = "192.0.2.1"
  }
}

resource "ibm_cbr_rule" "quarantine" {
  description      = "Quarantaine du bucket ${var.bucket_name} (clean programmé)"
  enforcement_mode = var.enforcement_mode

  contexts {
    attributes {
      name  = "networkZoneId"
      value = ibm_cbr_zone.quarantine.id
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
