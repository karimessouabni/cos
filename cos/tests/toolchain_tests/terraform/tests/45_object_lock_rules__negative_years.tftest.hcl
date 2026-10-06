# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « negative_years »
#
# Object lock : refus à la création : cas de refus « negative_years ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules negative_years"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (-1 years) cannot be inferior or equal to ZERO. »
run "negative_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = -1, immutability_choice = "object_lock_yearly" }
    }
  }
}
