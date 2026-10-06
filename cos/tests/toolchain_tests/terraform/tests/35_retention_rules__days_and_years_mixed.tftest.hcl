# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « days_and_years_mixed »
#
# Rétention : refus à la création : cas de refus « days_and_years_mixed ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules days_and_years_mixed"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention must be set either in days (…_days) or in years (…_years) for all three attributes, not a mix of both. »
run "days_and_years_mixed" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_years = 3 } }
    }
  }
}
