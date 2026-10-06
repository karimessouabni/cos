# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « years_over_five »
#
# Object lock : refus à la création : cas de refus « years_over_five ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules years_over_five"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (6 years) cannot be superior to 5 years. »
run "years_over_five" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = 6, immutability_choice = "object_lock_yearly" }
    }
  }
}
