# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « zero_days »
#
# Object lock : refus à la création : cas de refus « zero_days ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules zero_days"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (0 days) cannot be inferior or equal to ZERO. »
run "zero_days" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 0, immutability_choice = "object_lock_daily" }
    }
  }
}
