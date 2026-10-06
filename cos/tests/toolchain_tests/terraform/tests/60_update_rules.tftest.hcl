# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Mises à jour d'un bucket déjà immuable
#
# Un bucket en rétention et un en object lock ; ce qui est refusé (l'autre
# immutabilité, le versioning, la sauvegarde, bornes égales, deux unités, 5 ans
# dépassés) et ce qui est accepté (bornes dans l'autre unité, retention_enabled
# = false sans effet, changement d'unité de l'object lock).

variables {
  scenario   = "update rules"
  with_vault = true
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
    condition     = output.bucket_status["ret"] == null || output.bucket_status["ret"] != "DECLINED"
    error_message = "ret (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["ret"]}"
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
    condition     = output.bucket_status["lock"] == null || output.bucket_status["lock"] != "DECLINED"
    error_message = "lock (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["lock"]}"
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
#   ret : update -> REFUSÉ (service) « Setting an object-lock is not possible when retention is already enabled. »
run "ret_add_object_lock" {
  variables {
    buckets = {
      ret  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, object_lock_duration_days = 1 }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["ret"] == "DECLINED"
    error_message = "ret (update) : aurait dû être refusé (Setting an object-lock is not possible when retention is already enabled.) ; status = ${jsonencode(output.bucket_status["ret"])}."
  }
  assert {
    condition     = can(regex("Setting an object-lock is not possible when retention is already enabled\\.", output.bucket_status_reason["ret"]))
    error_message = "ret (update) : motif inattendu : ${output.bucket_status_reason["ret"]}"
  }
}

# Attendu :
#   ret : update -> REFUSÉ (service) « Enabling versioning is not possible when retention is already enabled. »
run "ret_enable_versioning" {
  variables {
    buckets = {
      ret  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, enable_versioning = true }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["ret"] == "DECLINED"
    error_message = "ret (update) : aurait dû être refusé (Enabling versioning is not possible when retention is already enabled.) ; status = ${jsonencode(output.bucket_status["ret"])}."
  }
  assert {
    condition     = can(regex("Enabling versioning is not possible when retention is already enabled\\.", output.bucket_status_reason["ret"]))
    error_message = "ret (update) : motif inattendu : ${output.bucket_status_reason["ret"]}"
  }
}

# Attendu :
#   ret : update -> REFUSÉ (service) « Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled. »
run "ret_add_backup" {
  variables {
    buckets = {
      ret  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, backup_retention_days = 7 }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["ret"] == "DECLINED"
    error_message = "ret (update) : aurait dû être refusé (Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled.) ; status = ${jsonencode(output.bucket_status["ret"])}."
  }
  assert {
    condition     = can(regex("Retention and bucket backup are not compatible\\. We cannot activate backup when retention is enabled\\.", output.bucket_status_reason["ret"]))
    error_message = "ret (update) : motif inattendu : ${output.bucket_status_reason["ret"]}"
  }
}

# Attendu :
#   ret : update -> REFUSÉ (service) « Retention default (5 days) cannot be inferior to minimum (5 days) nor superior or equal to maximum (5 days). | Retention maximum (5 days) cannot be inferior or equal to default (5 days) nor superior to 5 years (1826 days). »
run "ret_equal_bounds" {
  variables {
    buckets = {
      ret  = { retention = { minimum_days = 5, default_days = 5, maximum_days = 5 } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["ret"] == "DECLINED"
    error_message = "ret (update) : aurait dû être refusé (Retention default (5 days) cannot be inferior to minimum (5 days) nor superior or equal to maximum (5 days). | Retention maximum (5 days) cannot be inferior or equal to default (5 days) nor superior to 5 years (1826 days).) ; status = ${jsonencode(output.bucket_status["ret"])}."
  }
  assert {
    condition     = can(regex("Retention default \\(\\d+ days\\) cannot be inferior to minimum \\(\\d+ days\\) nor superior or equal to maximum \\(\\d+ days\\)\\. \\| Retention maximum \\(\\d+ days\\) cannot be inferior or equal to default \\(\\d+ days\\) nor superior to 5 years \\(\\d+ days\\)\\.", output.bucket_status_reason["ret"]))
    error_message = "ret (update) : motif inattendu : ${output.bucket_status_reason["ret"]}"
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
    condition     = output.bucket_status["ret"] == null || output.bucket_status["ret"] != "DECLINED"
    error_message = "ret (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["ret"]}"
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
    condition     = output.bucket_status["ret"] == null || output.bucket_status["ret"] != "DECLINED"
    error_message = "ret (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["ret"]}"
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
#   lock : update -> REFUSÉ (service) « Setting a retention is not possible when object-lock is already enabled. »
run "lock_add_retention" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["lock"] == "DECLINED"
    error_message = "lock (update) : aurait dû être refusé (Setting a retention is not possible when object-lock is already enabled.) ; status = ${jsonencode(output.bucket_status["lock"])}."
  }
  assert {
    condition     = can(regex("Setting a retention is not possible when object-lock is already enabled\\.", output.bucket_status_reason["lock"]))
    error_message = "lock (update) : motif inattendu : ${output.bucket_status_reason["lock"]}"
  }
}

# Attendu :
#   lock : update -> REFUSÉ (service) « Disabling versioning is not possible when object-lock is already enabled. »
run "lock_disable_versioning" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = false, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["lock"] == "DECLINED"
    error_message = "lock (update) : aurait dû être refusé (Disabling versioning is not possible when object-lock is already enabled.) ; status = ${jsonencode(output.bucket_status["lock"])}."
  }
  assert {
    condition     = can(regex("Disabling versioning is not possible when object-lock is already enabled\\.", output.bucket_status_reason["lock"]))
    error_message = "lock (update) : motif inattendu : ${output.bucket_status_reason["lock"]}"
  }
}

# Attendu :
#   lock : update -> REFUSÉ (service) « Object lock retention must be set either in days (object_lock_duration_days) or in years (object_lock_duration_years), not both. »
run "lock_both_units" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["lock"] == "DECLINED"
    error_message = "lock (update) : aurait dû être refusé (Object lock retention must be set either in days (object_lock_duration_days) or in years (object_lock_duration_years), not both.) ; status = ${jsonencode(output.bucket_status["lock"])}."
  }
  assert {
    condition     = can(regex("Object lock retention must be set either in days \\(object_lock_duration_days\\) or in years \\(object_lock_duration_years\\), not both\\.", output.bucket_status_reason["lock"]))
    error_message = "lock (update) : motif inattendu : ${output.bucket_status_reason["lock"]}"
  }
}

# Attendu :
#   lock : update -> REFUSÉ (service) « Object lock retention (1900 days) cannot be superior to 5 years (1826 days). »
run "lock_over_five_years" {
  variables {
    buckets = {
      ret  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3, retention_enabled = false } }
      lock = { enable_versioning = true, object_lock_duration_days = 1900, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["lock"] == "DECLINED"
    error_message = "lock (update) : aurait dû être refusé (Object lock retention (1900 days) cannot be superior to 5 years (1826 days).) ; status = ${jsonencode(output.bucket_status["lock"])}."
  }
  assert {
    condition     = can(regex("Object lock retention \\(\\d+ days\\) cannot be superior to 5 years \\(\\d+ days\\)\\.", output.bucket_status_reason["lock"]))
    error_message = "lock (update) : motif inattendu : ${output.bucket_status_reason["lock"]}"
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
    condition     = output.bucket_status["lock"] == null || output.bucket_status["lock"] != "DECLINED"
    error_message = "lock (update) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["lock"]}"
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
