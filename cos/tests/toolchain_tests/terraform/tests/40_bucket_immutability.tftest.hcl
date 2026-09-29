# Object lock : création avec durée 1 jour et versioning, update de la durée.

variables {
  scenario = "object lock"
}

run "create_object_lock" {
  variables {
    buckets = { locked = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 1 } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["locked"].payload.object_lock_duration_days == 1
    error_message = "object_lock_duration_days attendu : 1."
  }
}

run "update_object_lock_duration" {
  variables {
    buckets = { locked = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 2 } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["locked"].name == run.create_object_lock.bucket_names["locked"]
    error_message = "L'update d'object lock a recréé le bucket."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["locked"].payload.object_lock_duration_days == 2
    error_message = "object_lock_duration_days attendu : 2."
  }
}
