# Une création par classe de stockage, en un seul run (la classe n'est pas
# modifiable ensuite).

variables {
  scenario = "storage classes"
}

run "one_bucket_per_storage_class" {
  variables {
    buckets = {
      vault = { storage_class = "vault" }
      cold  = { storage_class = "cold" }
      smart = { storage_class = "smart" }
    }
  }

  assert {
    condition     = alltrue([for b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Les trois buckets n'ont pas tous été créés."
  }
  assert {
    condition     = { for k, b in orchestrator_subscription_cosbucket_v1.bucket : k => b.payload.storage_class } == { vault = "vault", cold = "cold", smart = "smart" }
    error_message = "storage_class non respectée."
  }
}
