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
    condition     = output.bucket_status["vault"] == null || output.bucket_status["vault"] != "DECLINED"
    error_message = "vault (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["vault"]}"
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
    condition     = output.bucket_status["cold"] == null || output.bucket_status["cold"] != "DECLINED"
    error_message = "cold (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["cold"]}"
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
    condition     = output.bucket_status["smart"] == null || output.bucket_status["smart"] != "DECLINED"
    error_message = "smart (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["smart"]}"
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

# Attendu :
#   glacier : create -> REFUSÉ (schema) « Input should be 'standard', 'vault', 'cold' or 'smart' »
run "unknown_class" {
  variables {
    buckets = {
      vault   = { storage_class = "vault" }
      cold    = { storage_class = "cold" }
      smart   = { storage_class = "smart" }
      glacier = { storage_class = "glacier" }
    }
  }

  assert {
    condition     = output.bucket_status["glacier"] == "DECLINED"
    error_message = "glacier (create) : aurait dû être refusé (Input should be 'standard', 'vault', 'cold' or 'smart') ; status = ${jsonencode(output.bucket_status["glacier"])}."
  }
  assert {
    condition     = can(regex("Input should be 'standard', 'vault', 'cold' or 'smart'", output.bucket_status_reason["glacier"]))
    error_message = "glacier (create) : motif inattendu : ${output.bucket_status_reason["glacier"]}"
  }
}
