# Cas de refus : le provider fait échouer l'apply quand le DAG refuse la demande.
# toolchain_env.py vérifie que la sortie porte le motif attendu (expected_failures.json).

variables {
  scenario = "refused retention with versioning"
}

run "refused" {
  variables {
    buckets = { r = { enable_versioning = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } } }
  }
}
