# Une création par classe de stockage (la classe n'est pas modifiable ensuite).

variables {
  scenario = "storage classes"
}

run "one_bucket_per_class" {
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
}
