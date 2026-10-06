# Object lock : en jours et en années (versioning requis), puis update de la durée.

variables {
  scenario = "object lock"
}

run "create" {
  variables {
    buckets = {
      daily  = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 1 }
      yearly = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_years = 1 }
    }
  }
  assert {
    condition     = alltrue([for b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Un bucket en object lock n'a pas été créé."
  }
}

run "update_duration" {
  variables {
    buckets = {
      daily  = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 2 }
      yearly = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_years = 5 }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name == run.create.bucket_names["daily"]
    error_message = "L'update d'object lock a recréé le bucket."
  }
}
