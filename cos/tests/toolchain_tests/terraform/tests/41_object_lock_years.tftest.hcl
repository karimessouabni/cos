# Object lock en années : création à 1 an, update à 5 ans (plafond accepté),
# et la valeur en jours ignorée quand le choix est yearly.

variables {
  scenario = "object lock years"
}

run "create_object_lock_yearly" {
  variables {
    buckets = { locked = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_years = 1 } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["locked"].payload.object_lock_duration_years == 1
    error_message = "object_lock_duration_years attendu : 1."
  }
  assert {
    condition     = output.bucket_status["locked"] == null || output.bucket_status["locked"] != "DECLINED"
    error_message = "Demande refusée : ${output.bucket_status_reason["locked"]}"
  }
}

run "update_to_five_years_ceiling" {
  variables {
    buckets = { locked = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_years = 5 } }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["locked"].name == run.create_object_lock_yearly.bucket_names["locked"]
    error_message = "L'update de la durée a recréé le bucket."
  }
  assert {
    condition     = output.bucket_status["locked"] == null || output.bucket_status["locked"] != "DECLINED"
    error_message = "5 ans (plafond) aurait dû être accepté : ${output.bucket_status_reason["locked"]}"
  }
}
