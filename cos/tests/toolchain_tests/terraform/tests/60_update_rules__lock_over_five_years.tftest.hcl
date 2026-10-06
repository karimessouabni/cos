# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Mises à jour d'un bucket déjà immuable : refus « lock_over_five_years »
#
# Mises à jour d'un bucket déjà immuable : cas de refus « lock_over_five_years
# ». Le provider fait échouer l'apply du run refusé ; toolchain_env.py vérifie
# que la sortie porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "update rules lock_over_five_years"
}

# Attendu :
#   lock : create -> ACCEPTÉ
run "create_base" {
  variables {
    buckets = {
      lock = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
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
#   lock : update -> REFUSÉ (service) « Object lock retention (1900 days) cannot be superior to 5 years (1826 days). »
run "lock_over_five_years" {
  variables {
    buckets = {
      lock = { enable_versioning = true, object_lock_duration_days = 1900, immutability_choice = "object_lock_daily" }
    }
  }
}
