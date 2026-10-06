# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention aux bornes acceptées
#
# Exactement 5 ans, 1826 jours (plafond en jours = 1826 ou 1827 selon les
# bissextiles, 1826 passe toujours), format historique au plafond, bornes
# égales. Puis un update quelconque du bucket aux bornes égales : la règle
# d'update (minimum < default < maximum, stricte) le refuse alors que la
# création l'avait accepté — incohérence à trancher côté DAG.

variables {
  scenario = "retention limits"
}

# Attendu :
#   five_years : create -> ACCEPTÉ
#   days_ceiling : create -> ACCEPTÉ
#   legacy_ceiling : create -> ACCEPTÉ
#   equal_bounds : create -> ACCEPTÉ
run "ceilings" {
  variables {
    buckets = {
      five_years     = { retention = { minimum_years = 1, default_years = 5, maximum_years = 5 } }
      days_ceiling   = { retention = { minimum_days = 1, default_days = 1826, maximum_days = 1826 } }
      legacy_ceiling = { retention = { minimum = 1, default = 1826, maximum = 1826 } }
      equal_bounds   = { retention = { minimum_days = 30, default_days = 30, maximum_days = 30 } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].name != ""
    error_message = "five_years (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.storage_class == "standard"
    error_message = "five_years (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention.minimum_years == 1
    error_message = "five_years (create) : payload.retention.minimum_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention.default_years == 5
    error_message = "five_years (create) : payload.retention.default_years attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention.maximum_years == 5
    error_message = "five_years (create) : payload.retention.maximum_years attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["five_years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].name != ""
    error_message = "days_ceiling (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.storage_class == "standard"
    error_message = "days_ceiling (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention.minimum_days == 1
    error_message = "days_ceiling (create) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention.default_days == 1826
    error_message = "days_ceiling (create) : payload.retention.default_days attendu 1826, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention.maximum_days == 1826
    error_message = "days_ceiling (create) : payload.retention.maximum_days attendu 1826, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].name != ""
    error_message = "legacy_ceiling (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.storage_class == "standard"
    error_message = "legacy_ceiling (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention.minimum == 1
    error_message = "legacy_ceiling (create) : payload.retention.minimum attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention.default == 1826
    error_message = "legacy_ceiling (create) : payload.retention.default attendu 1826, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention.maximum == 1826
    error_message = "legacy_ceiling (create) : payload.retention.maximum attendu 1826, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy_ceiling"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].name != ""
    error_message = "equal_bounds (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.storage_class == "standard"
    error_message = "equal_bounds (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention.minimum_days == 30
    error_message = "equal_bounds (create) : payload.retention.minimum_days attendu 30, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention.default_days == 30
    error_message = "equal_bounds (create) : payload.retention.default_days attendu 30, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention.maximum_days == 30
    error_message = "equal_bounds (create) : payload.retention.maximum_days attendu 30, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["equal_bounds"].payload.retention, null))}."
  }
}
