# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « daily_choice_without_values »
#
# Rétention : refus à la création : cas de refus « daily_choice_without_values
# ». Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie
# que la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules daily_choice_without_values"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must not be empty for choice retention_daily. »
run "daily_choice_without_values" {
  variables {
    buckets = {
      r = { immutability_choice = "retention_daily" }
    }
  }
}
