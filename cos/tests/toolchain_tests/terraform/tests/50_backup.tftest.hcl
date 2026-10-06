# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : vault, bucket sauvegardé, désactivation
#
# Le vault du scénario, un bucket sauvegardé, un bucket créé avec backup_enabled
# = false (équivaut à pas de sauvegarde) ; mise à jour de la rétention des
# sauvegardes, désactivation puis réactivation.

variables {
  scenario   = "backup"
  with_vault = true
}

# Attendu :
#   saved : create -> ACCEPTÉ
#   disabled_flag : create -> ACCEPTÉ
run "create" {
  variables {
    buckets = {
      saved         = { enable_versioning = true, backup_retention_days = 7 }
      disabled_flag = { enable_versioning = true, backup_enabled = false }
    }
  }

  assert {
    condition     = output.bucket_status["saved"] == null || output.bucket_status["saved"] != "DECLINED"
    error_message = "saved (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["saved"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name != ""
    error_message = "saved (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning == true
    error_message = "saved (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class == "standard"
    error_message = "saved (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_enabled == true
    error_message = "saved (create) : payload.backup.backup_enabled attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_retention_days == 7
    error_message = "saved (create) : payload.backup.backup_retention_days attendu 7, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
  assert {
    condition     = output.bucket_status["disabled_flag"] == null || output.bucket_status["disabled_flag"] != "DECLINED"
    error_message = "disabled_flag (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["disabled_flag"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].name != ""
    error_message = "disabled_flag (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.enable_versioning == true
    error_message = "disabled_flag (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.storage_class == "standard"
    error_message = "disabled_flag (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.backup.backup_enabled == false
    error_message = "disabled_flag (create) : payload.backup.backup_enabled attendu false, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.backup, null))}."
  }
}

# Attendu :
#   saved : update -> ACCEPTÉ
run "update_backup_retention" {
  variables {
    buckets = {
      saved         = { enable_versioning = true, backup_retention_days = 2 }
      disabled_flag = { enable_versioning = true, backup_enabled = false }
    }
  }

  assert {
    condition     = output.bucket_status["saved"] == null || output.bucket_status["saved"] != "DECLINED"
    error_message = "saved (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["saved"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name != ""
    error_message = "saved (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create.bucket_names["saved"]
    error_message = "saved (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning == true
    error_message = "saved (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class == "standard"
    error_message = "saved (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_enabled == true
    error_message = "saved (update) : payload.backup.backup_enabled attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_retention_days == 2
    error_message = "saved (update) : payload.backup.backup_retention_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
}

# Attendu :
#   saved : update -> ACCEPTÉ
run "disable_backup" {
  variables {
    buckets = {
      saved         = { enable_versioning = true, backup_enabled = false }
      disabled_flag = { enable_versioning = true, backup_enabled = false }
    }
  }

  assert {
    condition     = output.bucket_status["saved"] == null || output.bucket_status["saved"] != "DECLINED"
    error_message = "saved (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["saved"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name != ""
    error_message = "saved (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create.bucket_names["saved"]
    error_message = "saved (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning == true
    error_message = "saved (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class == "standard"
    error_message = "saved (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_enabled == false
    error_message = "saved (update) : payload.backup.backup_enabled attendu false, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
}

# Attendu :
#   saved : update -> ACCEPTÉ
run "enable_backup_again" {
  variables {
    buckets = {
      saved         = { enable_versioning = true, backup_retention_days = 7 }
      disabled_flag = { enable_versioning = true, backup_enabled = false }
    }
  }

  assert {
    condition     = output.bucket_status["saved"] == null || output.bucket_status["saved"] != "DECLINED"
    error_message = "saved (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["saved"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name != ""
    error_message = "saved (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].name == run.create.bucket_names["saved"]
    error_message = "saved (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning == true
    error_message = "saved (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class == "standard"
    error_message = "saved (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_enabled == true
    error_message = "saved (update) : payload.backup.backup_enabled attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup.backup_retention_days == 7
    error_message = "saved (update) : payload.backup.backup_retention_days attendu 7, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["saved"].payload.backup, null))}."
  }
}
