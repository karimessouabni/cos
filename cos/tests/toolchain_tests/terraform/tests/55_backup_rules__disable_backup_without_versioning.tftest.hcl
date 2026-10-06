# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « disable_backup_without_versioning »
#
# Sauvegarde : refus : cas de refus « disable_backup_without_versioning ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules disable_backup_without_versioning"
  with_vault = true
}

# Attendu :
#   plain : create -> ACCEPTÉ
run "create_base" {
  variables {
    buckets = {
      plain = {}
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["plain"].name != ""
    error_message = "plain (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["plain"].payload.storage_class == "standard"
    error_message = "plain (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["plain"].payload.storage_class, null))}."
  }
}

# Attendu :
#   plain : update -> REFUSÉ (service) « Versioning should be enabled to disable bucket backup. »
run "disable_backup_without_versioning" {
  variables {
    buckets = {
      plain = { backup_enabled = false }
    }
  }
}
