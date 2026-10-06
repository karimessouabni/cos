# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « minimum_above_maximum »
#
# Rétention : refus à la création : cas de refus « minimum_above_maximum ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules minimum_above_maximum"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention minimum cannot be superior to maximum. | Retention default cannot be inferior to minimum. | Retention default cannot be superior to maximum. »
run "minimum_above_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 3, default_days = 2, maximum_days = 1 } }
    }
  }
}
