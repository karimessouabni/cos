# Quarantaine d'un bucket pendant la période de grâce d'un clean
# (docs/adr/0003-periode-de-grace-du-clean.md).
#
# Une règle Context-Based Restrictions sur ce seul bucket : seuls les appels
# arrivant par les endpoints direct / private depuis la zone réseau de
# l'orchestrateur passent. Les clients (URL publique, clé API ou HMAC, proxy
# ou non) reçoivent 403. Airflow (endpoint direct) et Schematics (référence de
# service dans la zone) continuent de fonctionner.
#
# Posée par le DAG cos.bucket.v1.clean avec quarantine = true, levée par
# cos.bucket.v1.cancel_clean ou à la fin du clean avec quarantine = false :
# le même apply, la règle est dans le state du workspace, jamais orpheline.

variable "quarantine" {
  description = "Vrai pendant la période de grâce d'un clean : le bucket n'est plus joignable que par l'orchestrateur."
  type        = bool
  default     = false
}

variable "quarantine_allowed_vpc_crns" {
  description = "VPC de l'orchestrateur (Airflow / VPE) autorisés pendant la quarantaine. Aucun VPC client ne doit y figurer."
  type        = list(string)
  default     = []
}

variable "quarantine_enforcement_mode" {
  description = "enabled : bloque ; report : journalise seulement (CBR), pour valider la zone sur les premiers clients."
  type        = string
  default     = "enabled"
  validation {
    condition     = contains(["enabled", "report", "disabled"], var.quarantine_enforcement_mode)
    error_message = "quarantine_enforcement_mode doit valoir enabled, report ou disabled."
  }
}

locals {
  quarantine_count = var.quarantine ? 1 : 0
  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)
}

resource "ibm_cbr_zone" "quarantine" {
  count       = local.quarantine_count
  name        = "quarantine-${module.naming_bucket.name}"
  description = "Orchestrateur seulement, le temps de la période de grâce du clean"

  dynamic "addresses" {
    for_each = var.quarantine_allowed_vpc_crns
    content {
      type  = "vpc"
      value = addresses.value
    }
  }

  # Schematics applique ce même workspace : il doit rester autorisé.
  addresses {
    type = "serviceRef"
    ref {
      service_name = "schematics"
    }
  }
}

resource "ibm_cbr_rule" "quarantine" {
  count            = local.quarantine_count
  description      = "Quarantaine du bucket ${module.naming_bucket.name} (clean programmé)"
  enforcement_mode = var.quarantine_enforcement_mode

  contexts {
    attributes {
      name  = "networkZoneId"
      value = ibm_cbr_zone.quarantine[0].id
    }
    attributes {
      name  = "endpointType"
      value = "direct"
    }
  }
  contexts {
    attributes {
      name  = "networkZoneId"
      value = ibm_cbr_zone.quarantine[0].id
    }
    attributes {
      name  = "endpointType"
      value = "private"
    }
  }

  resources {
    attributes {
      name  = "accountId"
      value = data.ibm_iam_account_settings.quarantine[0].account_id
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
      value = module.naming_bucket.name
    }
  }
}

data "ibm_iam_account_settings" "quarantine" {
  count = local.quarantine_count
}
