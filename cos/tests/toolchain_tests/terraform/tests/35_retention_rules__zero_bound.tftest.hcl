# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « zero_bound »
#
# Rétention : refus à la création : cas de refus « zero_bound ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules zero_bound"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « minimum_days must be superior to 0. »
run "zero_bound" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 0, default_days = 1, maximum_days = 2 } }
    }
  }
}
