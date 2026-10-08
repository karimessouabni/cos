################################################################################
# Sonde du bucket depuis Schematics
################################################################################
# Pendant la quarantaine, seul Schematics passe la règle CBR : c'est donc d'ici
# que l'orchestrateur regarde le bucket. Un listing S3 d'un objet au plus, avec
# le jeton IAM du provider (hashicorp/http, aucune signature à calculer).
# Lu à chaque plan et apply tant que probe_enabled est vrai ; les sorties sont
# le statut HTTP (403 : Schematics est bloqué aussi) et bucket_empty.
# quarantine_test s'en sert pour prouver les deux faces de la règle ; le clean,
# pour savoir quand le vidage est terminé sans rouvrir le bucket.

data "ibm_iam_auth_token" "probe" {
  count = var.probe_enabled ? 1 : 0
}

locals {
  probe_endpoint = var.probe_endpoint != "" ? var.probe_endpoint : "https://s3.private.${var.region}.cloud-object-storage.appdomain.cloud/${var.bucket_name}"
  # Avec versioning, les versions non courantes et les delete markers comptent.
  probe_url   = var.probe_versions ? "${local.probe_endpoint}?versions&max-keys=1" : "${local.probe_endpoint}?list-type=2&max-keys=1"
  probe_token = var.probe_enabled ? data.ibm_iam_auth_token.probe[0].iam_access_token : ""
  probe_authorization = startswith(local.probe_token, "Bearer ") ? local.probe_token : "Bearer ${local.probe_token}"
  probe_status        = var.probe_enabled ? data.http.probe[0].status_code : null
  probe_body          = var.probe_enabled ? data.http.probe[0].response_body : ""
  probe_has_objects   = length(regexall("<(Contents|Version|DeleteMarker)>", local.probe_body)) > 0
}

data "http" "probe" {
  count = var.probe_enabled ? 1 : 0
  url   = local.probe_url
  request_headers = {
    Authorization = local.probe_authorization
  }
}

output "probe_url" {
  value = var.probe_enabled ? local.probe_url : null
}

output "probe_status_code" {
  description = "Statut HTTP du listing vu depuis Schematics (200 : Schematics passe ; 403 : bloqué)."
  value       = local.probe_status
}

output "bucket_empty" {
  description = "true si le listing a répondu 200 sans aucun objet, version ni delete marker ; null si la sonde est inactive ou refusée."
  value       = local.probe_status == 200 ? !local.probe_has_objects : null
}
