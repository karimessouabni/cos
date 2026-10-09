output "cbr_rule_id" {
  description = "Identifiant de la règle : sortie de secours `ibmcloud cbr rule-delete <id>`."
  value       = ibm_cbr_rule.quarantine.id
}

output "cbr_zone_id" {
  value = ibm_cbr_zone.quarantine.id
}

output "enforcement_mode" {
  description = "Mode appliqué à la règle : enabled (bucket fermé), report, ou disabled (bucket ouvert pour une action de l'orchestrateur)."
  value       = ibm_cbr_rule.quarantine.enforcement_mode
}
