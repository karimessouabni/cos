# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « default_below_minimum »
#
# Rétention : refus à la création : cas de refus « default_below_minimum ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules default_below_minimum"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention default cannot be inferior to minimum. »
run "default_below_minimum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 5, default_days = 2, maximum_days = 10 } }
    }
  }
}
