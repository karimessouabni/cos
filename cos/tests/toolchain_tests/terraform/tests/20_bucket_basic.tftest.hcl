# Cycle de vie d'un bucket standard : create -> update (versioning, permissions)
# -> destroy automatique (bucket puis cos, ordre inverse des dépendances).

variables {
  scenario = "bucket basic"
}

run "create_bucket" {
  variables {
    buckets = { basic = {} }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name != ""
    error_message = "Bucket non créé."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class == "standard"
    error_message = "storage_class attendue : standard."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.cos_instance == orchestrator_subscription_cos_v1.cos.name
    error_message = "Le bucket n'est pas rattaché à l'instance COS du scénario."
  }
}

run "update_enable_versioning" {
  variables {
    buckets = { basic = { enable_versioning = true } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create_bucket.bucket_names["basic"]
    error_message = "L'update a recréé le bucket au lieu de le modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_versioning == true
    error_message = "enable_versioning non pris en compte."
  }
}

run "update_custom_permissions" {
  variables {
    buckets = { basic = { enable_versioning = true, enable_custom_permissions = true } }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create_bucket.bucket_names["basic"]
    error_message = "L'update a recréé le bucket au lieu de le modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_custom_permissions == true
    error_message = "enable_custom_permissions non pris en compte."
  }
}
