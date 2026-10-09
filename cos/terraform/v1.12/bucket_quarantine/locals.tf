locals {
  # Identiques au workspace du bucket : chemin Vault de la clé API du compte workload.
  vault_namespace           = var.app_code
  vault_secret_path_account = "ibm/${var.wklapp_account_id}"

  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)

  # Sonde du bucket depuis Schematics (probe.tf). Avec versioning, les versions
  # non courantes et les delete markers comptent.
  probe_endpoint      = var.probe_endpoint != "" ? var.probe_endpoint : "https://s3.private.${var.region}.cloud-object-storage.appdomain.cloud/${var.bucket_name}"
  probe_url           = var.probe_versions ? "${local.probe_endpoint}?versions&max-keys=1" : "${local.probe_endpoint}?list-type=2&max-keys=1"
  probe_token         = var.probe_enabled ? data.ibm_iam_auth_token.probe[0].iam_access_token : ""
  probe_authorization = startswith(local.probe_token, "Bearer ") ? local.probe_token : "Bearer ${local.probe_token}"
  probe_status        = var.probe_enabled ? data.http.probe[0].status_code : null
  probe_body          = var.probe_enabled ? data.http.probe[0].response_body : ""
  probe_has_objects   = length(regexall("<(Contents|Version|DeleteMarker)>", local.probe_body)) > 0
}
