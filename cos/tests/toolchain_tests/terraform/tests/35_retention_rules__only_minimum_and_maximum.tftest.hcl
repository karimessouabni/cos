# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « only_minimum_and_maximum »
#
# Rétention : refus à la création : cas de refus « only_minimum_and_maximum ».
# Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que
# la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules only_minimum_and_maximum"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days (…_days) or in years (…_years). »
run "only_minimum_and_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, maximum_days = 3 } }
    }
  }
}
