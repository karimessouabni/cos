# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « unknown_choice »
#
# Object lock : refus à la création : cas de refus « unknown_choice ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules unknown_choice"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « 'object_lock_monthly' is not a valid Immutability »
run "unknown_choice" {
  variables {
    buckets = {
      r = { enable_versioning = true, immutability_choice = "object_lock_monthly" }
    }
  }
}
