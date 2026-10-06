# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « without_retention_days »
#
# Sauvegarde : refus : cas de refus « without_retention_days ». Le provider fait
# échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie porte
# le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules without_retention_days"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (service) « Bucket backup specifications are not fully set. »
run "without_retention_days" {
  variables {
    buckets = {
      r = { enable_versioning = true, backup_enabled = true }
    }
  }
}
