# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « both_units »
#
# Object lock : refus à la création : cas de refus « both_units ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules both_units"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set either in days (object_lock_duration_days) or in years (object_lock_duration_years), not both. »
run "both_units" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1 }
    }
  }
}
