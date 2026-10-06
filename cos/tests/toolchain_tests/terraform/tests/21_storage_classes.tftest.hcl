# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Classes de stockage
#
# Une création par classe (la classe n'est pas modifiable ensuite), puis une
# classe inconnue, refusée par le schéma du payload.

variables {
  scenario = "storage classes"
}

# Attendu :
#   vault : create -> ACCEPTÉ
#   cold : create -> ACCEPTÉ
#   smart : create -> ACCEPTÉ
run "one_bucket_per_class" {
  variables {
    buckets = {
      vault = { storage_class = "vault" }
      cold  = { storage_class = "cold" }
      smart = { storage_class = "smart" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["vault"].name != ""
    error_message = "vault (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["vault"].payload.storage_class == "vault"
    error_message = "vault (create) : payload.storage_class attendu \"vault\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["vault"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["cold"].name != ""
    error_message = "cold (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["cold"].payload.storage_class == "cold"
    error_message = "cold (create) : payload.storage_class attendu \"cold\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["cold"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["smart"].name != ""
    error_message = "smart (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["smart"].payload.storage_class == "smart"
    error_message = "smart (create) : payload.storage_class attendu \"smart\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["smart"].payload.storage_class, null))}."
  }
}
