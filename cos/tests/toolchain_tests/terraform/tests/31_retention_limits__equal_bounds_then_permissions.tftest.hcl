# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention aux bornes acceptées : refus « equal_bounds_then_permissions »
#
# Rétention aux bornes acceptées : cas de refus « equal_bounds_then_permissions
# ». Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie
# que la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "retention limits equal_bounds_then_permissions"
}

# Attendu :
#   equal_bounds : create -> ACCEPTÉ
run "create_base" {
  variables {
    buckets = {
      equal_bounds = { retention = { minimum_days = 30, default_days = 30, maximum_days = 30 } }
    }
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

# Attendu :
#   equal_bounds : update -> REFUSÉ (service) « Retention default (30 days) cannot be inferior to minimum (30 days) nor superior or equal to maximum (30 days). | Retention maximum (30 days) cannot be inferior or equal to default (30 days) nor superior to 5 years (1826 days). »
run "equal_bounds_then_permissions" {
  variables {
    buckets = {
      equal_bounds = { retention = { minimum_days = 30, default_days = 30, maximum_days = 30 }, enable_custom_permissions = true }
    }
  }
}
