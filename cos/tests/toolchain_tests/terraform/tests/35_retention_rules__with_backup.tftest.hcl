# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « with_backup »
#
# Rétention : refus à la création : cas de refus « with_backup ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "retention rules with_backup"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled. »
run "with_backup" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, backup_retention_days = 7 }
    }
  }
}
