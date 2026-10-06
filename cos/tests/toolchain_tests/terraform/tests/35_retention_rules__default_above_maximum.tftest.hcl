# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « default_above_maximum »
#
# Rétention : refus à la création : cas de refus « default_above_maximum ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules default_above_maximum"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention default cannot be superior to maximum. »
run "default_above_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 20, maximum_days = 10 } }
    }
  }
}
