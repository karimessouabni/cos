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
# La règle ne laisse passer que Schematics du compte hub (référence de
# service) : le client est bloqué, et le vidage peut se faire depuis ce
# workspace, bucket toujours fermé (probe.tf). Sans identifiant du compte hub,
# la zone retombe sur une adresse qui ne correspond à rien (documentation,
# RFC 5737) et la règle bloque tout, Schematics compris.

locals {
  # Compte workload qui possède le bucket : celui du realm, passé par le DAG
  # (même valeur que le chemin Vault de la clé API). Aucun appel IAM.
  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)
  allow_schematics  = var.hub_account_id != ""
  quarantine_scope  = local.allow_schematics ? "schematics" : "none"
}

resource "ibm_cbr_zone" "quarantine" {
  name        = "quarantine-${var.bucket_name}"
  description = local.allow_schematics ? "Quarantaine : seul Schematics du compte hub passe" : "Zone vide (adresse de documentation) : aucune requête ne la satisfait"
  account_id  = var.wklapp_account_id

  # Référence de service : les requêtes émises par IBM Schematics depuis le
  # compte hub (les workspaces de l'orchestrateur, celui-ci compris).
  dynamic "addresses" {
    for_each = local.allow_schematics ? [1] : []
    content {
      type = "serviceRef"
      ref {
        account_id   = var.hub_account_id
        service_name = "schematics"
      }
    }
  }

  dynamic "addresses" {
    for_each = local.allow_schematics ? [] : [1]
    content {
      type  = "ipAddress"
      value = "192.0.2.1"
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
      value = var.wklapp_account_id
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

output "quarantine_scope" {
  description = "schematics : Schematics du compte hub passe ; none : tout est bloqué."
  value       = local.quarantine_scope
}
