# Backup vault -> bucket sauvegardé -> update de la rétention des sauvegardes -> désactivation.

variables {
  scenario   = "backup"
  with_vault = true
}

run "create" {
  variables {
    buckets = { saved = { enable_versioning = true, backup_retention_days = 7 } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name != ""
    error_message = "Bucket sauvegardé non créé."
  }
}

run "update_backup_retention" {
  variables {
    buckets = { saved = { enable_versioning = true, backup_retention_days = 2 } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create.bucket_names["saved"]
    error_message = "L'update de backup a recréé le bucket."
  }
}

run "disable_backup" {
  variables {
    buckets = { saved = { enable_versioning = true, backup_enabled = false } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create.bucket_names["saved"]
    error_message = "La désactivation du backup a recréé le bucket."
  }
}
