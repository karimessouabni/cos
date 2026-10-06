# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « yearly_choice_without_years »
#
# Object lock : refus à la création : cas de refus « yearly_choice_without_years
# ». Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie
# que la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "object lock rules yearly_choice_without_years"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set in years (object_lock_duration_years) for choice object_lock_yearly. »
run "yearly_choice_without_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }
}
