# Rétention aux bornes acceptées (ADR 0001) : exactement 5 ans en années,
# 1826 jours (plafond en jours = 1826 ou 1827 selon les bissextiles, 1826 passe
# toujours), format historique au plafond, bornes égales.

variables {
  scenario = "retention limits"
}

run "ceilings_are_accepted" {
  variables {
    buckets = {
      five_years   = { retention = { minimum_years = 1, default_years = 5, maximum_years = 5 } }
      days_ceiling = { retention = { minimum_days = 1, default_days = 1826, maximum_days = 1826 } }
      legacy_days  = { retention = { minimum = 1, default = 1826, maximum = 1826 } }
      equal_bounds = { retention = { minimum_days = 30, default_days = 30, maximum_days = 30 } }
    }
  }

  assert {
    condition     = alltrue([for k, b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Un bucket aux bornes limites n'a pas été créé."
  }
  assert {
    condition     = alltrue([for k, s in output.bucket_status : s == null || s != "DECLINED"])
    error_message = "Une demande aux bornes limites a été refusée : ${jsonencode(output.bucket_status_reason)}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention.default_years == 5
    error_message = "default_years attendu : 5."
  }
}
