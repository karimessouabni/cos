# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « days_over_five_years »
#
# Object lock : refus à la création : cas de refus « days_over_five_years ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules days_over_five_years"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (1900 days) cannot be superior to 5 years (1826 days). »
run "days_over_five_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1900, immutability_choice = "object_lock_daily" }
    }
  }
}
