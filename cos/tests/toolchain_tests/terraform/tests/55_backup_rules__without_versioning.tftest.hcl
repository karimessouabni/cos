# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « without_versioning »
#
# Sauvegarde : refus : cas de refus « without_versioning ». Le provider fait
# échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie porte
# le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules without_versioning"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (service) « Versioning should be enabled to enable bucket backup. »
run "without_versioning" {
  variables {
    buckets = {
      r = { enable_versioning = false, backup_retention_days = 7 }
    }
  }
}
