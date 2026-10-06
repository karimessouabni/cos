# Bucket standard : create -> update versioning -> update permissions -> destroy.

variables {
  scenario = "lifecycle"
}

run "create" {
  variables {
    buckets = { basic = {} }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name != ""
    error_message = "Bucket non créé."
  }
}

run "update_versioning" {
  variables {
    buckets = { basic = { enable_versioning = true } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create.bucket_names["basic"]
    error_message = "L'update a recréé le bucket."
  }
}

run "update_permissions" {
  variables {
    buckets = { basic = { enable_versioning = true, enable_custom_permissions = true } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create.bucket_names["basic"]
    error_message = "L'update a recréé le bucket."
  }
}
