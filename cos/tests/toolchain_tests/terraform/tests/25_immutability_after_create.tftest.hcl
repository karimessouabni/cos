# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Immutabilité ajoutée après la création
#
# Un bucket créé sans immutabilité peut en recevoir une par update : rétention
# (bucket vide, sans versioning), object lock (versioning requis), sauvegarde
# (versioning requis, vault du scénario).

variables {
  scenario   = "immutability after create"
  with_vault = true
}

# Attendu :
#   to_retention : create -> ACCEPTÉ
#   to_object_lock : create -> ACCEPTÉ
#   to_backup : create -> ACCEPTÉ
run "create_plain_buckets" {
  variables {
    buckets = {
      to_retention   = {}
      to_object_lock = { enable_versioning = true }
      to_backup      = { enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["to_retention"] == null || output.bucket_status["to_retention"] != "DECLINED"
    error_message = "to_retention (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_retention"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].name != ""
    error_message = "to_retention (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.storage_class == "standard"
    error_message = "to_retention (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.storage_class, null))}."
  }
  assert {
    condition     = output.bucket_status["to_object_lock"] == null || output.bucket_status["to_object_lock"] != "DECLINED"
    error_message = "to_object_lock (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_object_lock"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].name != ""
    error_message = "to_object_lock (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.enable_versioning == true
    error_message = "to_object_lock (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.storage_class == "standard"
    error_message = "to_object_lock (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.storage_class, null))}."
  }
  assert {
    condition     = output.bucket_status["to_backup"] == null || output.bucket_status["to_backup"] != "DECLINED"
    error_message = "to_backup (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_backup"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].name != ""
    error_message = "to_backup (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.enable_versioning == true
    error_message = "to_backup (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.storage_class == "standard"
    error_message = "to_backup (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.storage_class, null))}."
  }
}

# Attendu :
#   to_retention : update -> ACCEPTÉ
run "add_retention" {
  variables {
    buckets = {
      to_retention   = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      to_object_lock = { enable_versioning = true }
      to_backup      = { enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["to_retention"] == null || output.bucket_status["to_retention"] != "DECLINED"
    error_message = "to_retention (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_retention"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].name != ""
    error_message = "to_retention (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].name == run.create_plain_buckets.bucket_names["to_retention"]
    error_message = "to_retention (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.storage_class == "standard"
    error_message = "to_retention (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention.minimum_days == 1
    error_message = "to_retention (update) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention.default_days == 2
    error_message = "to_retention (update) : payload.retention.default_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention.maximum_days == 3
    error_message = "to_retention (update) : payload.retention.maximum_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_retention"].payload.retention, null))}."
  }
}

# Attendu :
#   to_object_lock : update -> ACCEPTÉ
run "add_object_lock" {
  variables {
    buckets = {
      to_retention   = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      to_object_lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
      to_backup      = { enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["to_object_lock"] == null || output.bucket_status["to_object_lock"] != "DECLINED"
    error_message = "to_object_lock (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_object_lock"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].name != ""
    error_message = "to_object_lock (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].name == run.create_plain_buckets.bucket_names["to_object_lock"]
    error_message = "to_object_lock (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.enable_versioning == true
    error_message = "to_object_lock (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.immutability_choice == "object_lock_daily"
    error_message = "to_object_lock (update) : payload.immutability_choice attendu \"object_lock_daily\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.object_lock_duration_days == 1
    error_message = "to_object_lock (update) : payload.object_lock_duration_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.storage_class == "standard"
    error_message = "to_object_lock (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_object_lock"].payload.storage_class, null))}."
  }
}

# Attendu :
#   to_backup : update -> ACCEPTÉ
run "add_backup" {
  variables {
    buckets = {
      to_retention   = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      to_object_lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
      to_backup      = { enable_versioning = true, backup_retention_days = 7 }
    }
  }

  assert {
    condition     = output.bucket_status["to_backup"] == null || output.bucket_status["to_backup"] != "DECLINED"
    error_message = "to_backup (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["to_backup"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].name != ""
    error_message = "to_backup (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].name == run.create_plain_buckets.bucket_names["to_backup"]
    error_message = "to_backup (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.enable_versioning == true
    error_message = "to_backup (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.storage_class == "standard"
    error_message = "to_backup (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.backup.backup_enabled == true
    error_message = "to_backup (update) : payload.backup.backup_enabled attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.backup, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.backup.backup_retention_days == 7
    error_message = "to_backup (update) : payload.backup.backup_retention_days attendu 7, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["to_backup"].payload.backup, null))}."
  }
}
