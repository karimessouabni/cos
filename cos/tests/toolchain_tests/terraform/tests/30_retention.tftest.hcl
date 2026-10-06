# Rétention (ADR 0001) : jours, années et format historique, puis update des bornes.

variables {
  scenario = "retention"
}

run "create" {
  variables {
    buckets = {
      days   = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      years  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
      legacy = { retention = { minimum = 1, default = 2, maximum = 3 } }
    }
  }
  assert {
    condition     = alltrue([for b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Un bucket en rétention n'a pas été créé."
  }
}

run "update_days_bounds" {
  variables {
    buckets = {
      days   = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      years  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
      legacy = { retention = { minimum = 1, default = 2, maximum = 3 } }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name == run.create.bucket_names["days"]
    error_message = "L'update de rétention a recréé le bucket."
  }
}
