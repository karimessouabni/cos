# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « negative_bound »
#
# Rétention : refus à la création : cas de refus « negative_bound ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules negative_bound"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « minimum_years must be superior to 0. »
run "negative_bound" {
  variables {
    buckets = {
      r = { retention = { minimum_years = -1, default_years = 1, maximum_years = 2 } }
    }
  }
}
