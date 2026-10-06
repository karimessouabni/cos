# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « years_over_five »
#
# Rétention : refus à la création : cas de refus « years_over_five ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules years_over_five"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « default_years (6 years) cannot be superior to 5 years (5 years, leap years included). | maximum_years (6 years) cannot be superior to 5 years (5 years, leap years included). »
run "years_over_five" {
  variables {
    buckets = {
      r = { retention = { minimum_years = 1, default_years = 6, maximum_years = 6 } }
    }
  }
}
