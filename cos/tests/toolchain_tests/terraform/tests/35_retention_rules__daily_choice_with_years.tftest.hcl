# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « daily_choice_with_years »
#
# Rétention : refus à la création : cas de refus « daily_choice_with_years ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules daily_choice_with_years"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must be set in days (default_days, minimum_days, maximum_days) for choice retention_daily. »
run "daily_choice_with_years" {
  variables {
    buckets = {
      r = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_daily" }
    }
  }
}
