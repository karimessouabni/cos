# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « without_versioning »
#
# Object lock : refus à la création : cas de refus « without_versioning ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules without_versioning"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Versioning should be enabled to enable object-lock. »
run "without_versioning" {
  variables {
    buckets = {
      r = { enable_versioning = false, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }
}
