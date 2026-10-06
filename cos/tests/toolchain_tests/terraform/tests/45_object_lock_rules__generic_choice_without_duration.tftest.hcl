# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création : refus « generic_choice_without_duration »
#
# Object lock : refus à la création : cas de refus «
# generic_choice_without_duration ». Le provider fait échouer l'apply du run
# refusé ; toolchain_env.py vérifie que la sortie porte le motif du DAG
# (tests/expected_failures.json).

variables {
  scenario = "object lock rules generic_choice_without_duration"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object Lock Duration Days or Years must be not empty. »
run "generic_choice_without_duration" {
  variables {
    buckets = {
      r = { enable_versioning = true, immutability_choice = "object_lock" }
    }
  }
}
