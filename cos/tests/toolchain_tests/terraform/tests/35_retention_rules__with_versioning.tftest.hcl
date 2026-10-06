# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « with_versioning »
#
# Rétention : refus à la création : cas de refus « with_versioning ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules with_versioning"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and versioning are not compatible. We cannot activate both of them simultaneously. »
run "with_versioning" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, enable_versioning = true }
    }
  }
}
