# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Mises à jour d'un bucket déjà immuable : refus « ret_equal_bounds »
#
# Mises à jour d'un bucket déjà immuable : cas de refus « ret_equal_bounds ». Le
# provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la
# sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "update rules ret_equal_bounds"
}

# Attendu :
#   ret : create -> ACCEPTÉ
run "create_base" {
  variables {
    buckets = {
      ret = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
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
}

# Attendu :
#   ret : update -> REFUSÉ (service) « Retention default (5 days) cannot be inferior to minimum (5 days) nor superior or equal to maximum (5 days). | Retention maximum (5 days) cannot be inferior or equal to default (5 days) nor superior to 5 years (1826 days). »
run "ret_equal_bounds" {
  variables {
    buckets = {
      ret = { retention = { minimum_days = 5, default_days = 5, maximum_days = 5 } }
    }
  }
}
