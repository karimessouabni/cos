# Rétention (ADR 0001) : un bucket en jours, un en années et un au format
# historique (default / minimum / maximum = jours implicites, déprécié mais
# accepté), créés ensemble, puis mise à jour des bornes du bucket en jours
# (les deux autres ne doivent pas bouger).
# Le "second plan sans changement" se vérifie avec
# toolchain_env.py --run plan -- -detailed-exitcode (code 2 = drift).

variables {
  scenario = "retention"
}

run "create_days_and_years" {
  variables {
    buckets = {
      days   = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      years  = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
      legacy = { retention = { minimum = 1, default = 2, maximum = 3 } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.default == 2 && !contains(keys(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention), "default_days")
    error_message = "Le format historique doit partir tel quel (default / minimum / maximum) : c'est le DAG qui le convertit en *_days."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.default_days == 2
    error_message = "retention.default_days attendu : 2."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.default_years == 2
    error_message = "retention.default_years attendu : 2."
  }
  assert {
    condition     = !contains(keys(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention), "default_days")
    error_message = "Le bucket en années ne doit porter aucune borne en jours."
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
    condition     = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => b.name } == run.create_days_and_years.bucket_names
    error_message = "L'update de rétention a recréé un bucket."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.default_days == 5 && orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.maximum_days == 10
    error_message = "Bornes de rétention non mises à jour."
  }
}
