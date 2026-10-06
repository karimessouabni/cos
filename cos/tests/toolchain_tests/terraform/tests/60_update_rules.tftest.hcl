# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Mises à jour d'un bucket déjà immuable
#
# Un bucket en rétention et un en object lock ; ce qui est refusé (l'autre
# immutabilité, le versioning, la sauvegarde, bornes égales, deux unités, 5 ans
# dépassés) et ce qui est accepté (bornes dans l'autre unité, retention_enabled
# = false sans effet, changement d'unité de l'object lock).

variables {
  scenario = "update rules"
}

# Attendu :
#   ret : create -> ACCEPTÉ
#   lock : create -> ACCEPTÉ
run "create" {
  variables {
    buckets = {
      ret  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].name != ""
    error_message = "ret (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class == "standard"
    error_message = "ret (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.minimum_days == 1
    error_message = "ret (create) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.default_days == 2
    error_message = "ret (create) : payload.retention.default_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.maximum_days == 3
    error_message = "ret (create) : payload.retention.maximum_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].name != ""
    error_message = "lock (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.enable_versioning == true
    error_message = "lock (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.immutability_choice == "object_lock_daily"
    error_message = "lock (create) : payload.immutability_choice attendu \"object_lock_daily\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.object_lock_duration_days == 1
    error_message = "lock (create) : payload.object_lock_duration_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.storage_class == "standard"
    error_message = "lock (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.storage_class, null))}."
  }
}

# Attendu :
#   ret : update -> ACCEPTÉ
run "ret_bounds_in_years" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].name != ""
    error_message = "ret (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].name == run.create.bucket_names["ret"]
    error_message = "ret (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class == "standard"
    error_message = "ret (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.minimum_years == 1
    error_message = "ret (update) : payload.retention.minimum_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.default_years == 2
    error_message = "ret (update) : payload.retention.default_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.maximum_years == 3
    error_message = "ret (update) : payload.retention.maximum_years attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
}

# retention_enabled = false : les bornes en base sont conservées, rien ne
# change.
# Attendu :
#   ret : update -> ACCEPTÉ
run "ret_disabled_flag_keeps_bounds" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].name != ""
    error_message = "ret (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].name == run.create.bucket_names["ret"]
    error_message = "ret (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class == "standard"
    error_message = "ret (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.minimum_years == 1
    error_message = "ret (update) : payload.retention.minimum_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.default_years == 2
    error_message = "ret (update) : payload.retention.default_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.maximum_years == 3
    error_message = "ret (update) : payload.retention.maximum_years attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention.retention_enabled == false
    error_message = "ret (update) : payload.retention.retention_enabled attendu false, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["ret"].payload.retention, null))}."
  }
}

# Attendu :
#   lock : update -> ACCEPTÉ
run "lock_switch_to_years" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].name != ""
    error_message = "lock (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].name == run.create.bucket_names["lock"]
    error_message = "lock (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.enable_versioning == true
    error_message = "lock (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.immutability_choice == "object_lock_yearly"
    error_message = "lock (update) : payload.immutability_choice attendu \"object_lock_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.object_lock_duration_years == 1
    error_message = "lock (update) : payload.object_lock_duration_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.storage_class == "standard"
    error_message = "lock (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["lock"].payload.storage_class, null))}."
  }
}
