# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « with_retention_and_versioning »
#
# Sauvegarde : refus : cas de refus « with_retention_and_versioning ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules with_retention_and_versioning"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and versioning are not compatible. We cannot activate both of them simultaneously. | Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled. »
run "with_retention_and_versioning" {
  variables {
    buckets = {
      r = { enable_versioning = true, backup_retention_days = 7, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
}
