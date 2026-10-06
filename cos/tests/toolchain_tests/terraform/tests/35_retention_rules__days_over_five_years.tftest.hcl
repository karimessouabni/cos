# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « days_over_five_years »
#
# Rétention : refus à la création : cas de refus « days_over_five_years ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules days_over_five_years"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « default_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). | maximum_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). »
run "days_over_five_years" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } }
    }
  }
}
