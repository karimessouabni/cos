# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « update_with_unknown_vault »
#
# Sauvegarde : refus : cas de refus « update_with_unknown_vault ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules update_with_unknown_vault"
  with_vault = true
}

# Attendu :
#   versioned : create -> ACCEPTÉ
run "create_base" {
  variables {
    buckets = {
      versioned = { enable_versioning = true }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].name != ""
    error_message = "versioned (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning == true
    error_message = "versioned (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class == "standard"
    error_message = "versioned (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class, null))}."
  }
}

# Attendu :
#   versioned : update -> REFUSÉ (dag) « No Backup Vault exist with the name : vault-inconnu-toolchain »
run "update_with_unknown_vault" {
  variables {
    buckets = {
      versioned = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "vault-inconnu-toolchain" }
    }
  }
}
