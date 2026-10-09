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
# La règle ne laisse passer que la référence de service Schematics : le client
# est bloqué, et l'orchestrateur regarde et vide le bucket depuis ce workspace,
# bucket toujours fermé (probe.tf).
#
# CBR refuse une référence de service d'un autre compte que celui de la zone
# ("Invalid `serviceRef` value: `account_id` ... Expected value is: <compte de
# la zone>") : elle porte donc le compte workload, pas le compte hub. À valider
# avec la sonde (probe_status_code) : si les jobs de l'agent Schematics du hub
# ne sont pas reconnus (403), passer à une adresse vpc ou IP.

resource "ibm_cbr_zone" "quarantine" {
  name        = "quarantine-${var.bucket_name}"
  description = "Quarantaine du bucket ${var.bucket_name} : seul Schematics passe"
  account_id  = var.cbr_account_id

  # Les requêtes émises par IBM Schematics. Le compte de la référence est
  # obligatoirement celui de la zone (compte workload).
  addresses {
    type = "serviceRef"
    ref {
      account_id   = var.cbr_account_id
      service_name = "schematics"
    }
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
