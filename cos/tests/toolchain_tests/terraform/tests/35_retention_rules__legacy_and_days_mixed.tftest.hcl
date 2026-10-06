# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création : refus « legacy_and_days_mixed »
#
# Rétention : refus à la création : cas de refus « legacy_and_days_mixed ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention rules legacy_and_days_mixed"
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention must use either the legacy fields (default, minimum, maximum) or the unit-suffixed fields (default_days, maximum_days), not both. »
run "legacy_and_days_mixed" {
  variables {
    buckets = {
      r = { retention = { minimum = 1, default_days = 2, maximum_days = 3 } }
    }
  }
}
