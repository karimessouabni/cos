################################################################################
# Quarantaine d'un bucket pendant la période de grâce d'un clean
# (docs/adr/0003-periode-de-grace-du-clean.md, docs/adr/0004-quarantaine-cbr-blocage-total.md)
################################################################################
# Workspace séparé du bucket : son state ne contient que la zone et la règle
# CBR. Son refresh n'appelle que l'API CBR, qu'une règle CBR sur COS ne bloque
# jamais : il reste pilotable quoi que la règle bloque, et la lever est toujours
# possible. Le bucket n'est jamais lu ici, son nom et son instance arrivent en
# variables depuis la base de l'orchestrateur.
#
# La règle bloque tout : son seul contexte est une zone qui ne correspond à
# rien (adresse de documentation, RFC 5737, jamais routée). Aucune zone ne peut
# laisser passer l'orchestrateur sans laisser passer les clients : ses jobs et
# les applications arrivent chez COS par le même VPE (ADC), voir
# docs/adr/0004-quarantaine-cbr-blocage-total.md. L'orchestrateur désactive
# donc la règle quelques minutes quand il doit agir sur le bucket
# (rule_active = false), puis la réactive.

resource "ibm_cbr_zone" "quarantine" {
  name        = "quarantine-${var.bucket_name}"
  description = "Zone vide (adresse de documentation) : aucune requête ne la satisfait"
  account_id  = var.cbr_account_id

  addresses {
    type  = "ipAddress"
    value = "192.0.2.1"
  }
}

resource "ibm_cbr_rule" "quarantine" {
  description      = "Quarantaine du bucket ${var.bucket_name} (clean programmé)"
  enforcement_mode = local.effective_enforcement_mode

  contexts {
    attributes {
      name  = "networkZoneId"
      value = ibm_cbr_zone.quarantine.id
    }
  }

  resources {
    attributes {
      name  = "accountId"
      value = var.cbr_account_id
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
