################################################################################
# Sonde du bucket depuis Schematics
################################################################################
# Pendant la quarantaine, seul Schematics passe la règle CBR : c'est donc d'ici
# que l'orchestrateur regarde le bucket. Un listing S3 d'un objet au plus, avec
# le jeton IAM du provider (hashicorp/http, aucune signature à calculer).
# Lu à chaque plan et apply tant que probe_enabled est vrai ; sorties
# probe_status_code (403 : Schematics est bloqué aussi) et bucket_empty.
# quarantine_test s'en sert pour prouver les deux faces de la règle ; le clean,
# pour savoir quand le vidage est terminé sans rouvrir le bucket.

data "ibm_iam_auth_token" "probe" {
  count = var.probe_enabled ? 1 : 0
}

data "http" "probe" {
  count = var.probe_enabled ? 1 : 0
  url   = local.probe_url
  request_headers = {
    Authorization = local.probe_authorization
  }
}
