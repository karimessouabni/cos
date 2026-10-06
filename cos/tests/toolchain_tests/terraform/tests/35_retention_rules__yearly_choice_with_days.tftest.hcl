# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « yearly_choice_with_days »
#
# Rétention : refus à la création : cas de refus « yearly_choice_with_days ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules yearly_choice_with_days"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must be set in years (default_years, minimum_years, maximum_years) for choice retention_yearly. »
run "yearly_choice_with_days" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_yearly" }
    }
  }
}
