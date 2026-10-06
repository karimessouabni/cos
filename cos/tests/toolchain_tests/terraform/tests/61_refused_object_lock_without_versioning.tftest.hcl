# Cas de refus : le provider fait échouer l'apply quand le DAG refuse la demande.
# toolchain_env.py vérifie que la sortie porte le motif attendu (expected_failures.json).

variables {
  scenario = "refused object lock without versioning"
}

run "refused" {
  variables {
    buckets = { r = { object_lock_duration_days = 1 } }
  }
}
