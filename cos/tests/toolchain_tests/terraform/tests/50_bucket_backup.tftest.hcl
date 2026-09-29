# Backup vault : cos -> vault -> bucket sauvegardé, update de la rétention des
# sauvegardes. Destruction automatique en ordre inverse (bucket, vault, cos).

variables {
  scenario   = "backup"
  with_vault = true
}

run "create_vault" {
  assert {
    condition     = one(orchestrator_subscription_cosbackup_vault_v1.vault[*].id) != ""
    error_message = "Backup vault non créé."
  }
}

run "create_bucket_with_backup" {
  variables {
    buckets = { saved = { enable_versioning = true, backup_retention_days = 1 } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_enabled == true
    error_message = "backup_enabled attendu : true."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_vault_sub_id == orchestrator_subscription_cosbackup_vault_v1.vault[0].id
    error_message = "Le bucket n'est pas rattaché au backup vault."
  }
}

run "update_backup_retention" {
  variables {
    buckets = { saved = { enable_versioning = true, backup_retention_days = 2 } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create_bucket_with_backup.bucket_names["saved"]
    error_message = "L'update de backup a recréé le bucket."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_retention_days == 2
    error_message = "backup_retention_days attendu : 2."
  }
}
