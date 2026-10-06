# Cas de refus : le provider fait échouer l'apply quand le DAG refuse la demande.
# toolchain_env.py vérifie que la sortie porte le motif attendu (expected_failures.json).

variables {
  scenario = "refused retention over five years"
}

run "refused" {
  variables {
    buckets = { r = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } } }
  }
}
