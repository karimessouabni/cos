# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « daily_choice_without_days »
#
# Object lock : refus à la création : cas de refus « daily_choice_without_days
# ». Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie
# que la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules daily_choice_without_days"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set in days (object_lock_duration_days) for choice object_lock_daily. »
run "daily_choice_without_days" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_daily" }
    }
  }
}
