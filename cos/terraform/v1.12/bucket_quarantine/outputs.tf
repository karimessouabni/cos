output "cbr_rule_id" {
  description = "Identifiant de la règle : sortie de secours `ibmcloud cbr rule-delete <id>`."
  value       = ibm_cbr_rule.quarantine.id
}

output "cbr_zone_id" {
  value = ibm_cbr_zone.quarantine.id
}

output "probe_url" {
  value = var.probe_enabled ? local.probe_url : null
}

output "probe_status_code" {
  description = "Statut HTTP du listing vu depuis Schematics (200 attendu : Schematics passe la règle)."
  value       = local.probe_status
}

output "bucket_empty" {
  description = "true si le listing a répondu 200 sans aucun objet, version ni delete marker ; null si la sonde est inactive ou refusée."
  value       = local.probe_status == 200 ? !local.probe_has_objects : null
}
