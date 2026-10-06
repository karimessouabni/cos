# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : jours, années, choix explicite ou déduit
#
# Créations avec chaque choix (daily, yearly, générique, déduit sans choix) et
# un payload yearly qui porte aussi des jours (ignorés) ; mises à jour de la
# durée, jusqu'au plafond, et changement d'unité.

variables {
  scenario = "object lock"
}

# Attendu :
#   daily : create -> ACCEPTÉ
#   yearly : create -> ACCEPTÉ
#   generic_choice_days : create -> ACCEPTÉ
#   generic_choice_years : create -> ACCEPTÉ
#   inferred_years : create -> ACCEPTÉ
#   yearly_ignores_days : create -> ACCEPTÉ
run "create" {
  variables {
    buckets = {
      daily                = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
      yearly               = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_yearly" }
      generic_choice_days  = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock" }
      generic_choice_years = { enable_versioning = true, object_lock_duration_years = 2, immutability_choice = "object_lock" }
      inferred_years       = { enable_versioning = true, object_lock_duration_years = 2 }
      yearly_ignores_days  = { enable_versioning = true, object_lock_duration_years = 1, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name != ""
    error_message = "daily (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning == true
    error_message = "daily (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice == "object_lock_daily"
    error_message = "daily (create) : payload.immutability_choice attendu \"object_lock_daily\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_days == 1
    error_message = "daily (create) : payload.object_lock_duration_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class == "standard"
    error_message = "daily (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].name != ""
    error_message = "yearly (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.enable_versioning == true
    error_message = "yearly (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.immutability_choice == "object_lock_yearly"
    error_message = "yearly (create) : payload.immutability_choice attendu \"object_lock_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.object_lock_duration_years == 1
    error_message = "yearly (create) : payload.object_lock_duration_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.storage_class == "standard"
    error_message = "yearly (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].name != ""
    error_message = "generic_choice_days (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.enable_versioning == true
    error_message = "generic_choice_days (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.immutability_choice == "object_lock"
    error_message = "generic_choice_days (create) : payload.immutability_choice attendu \"object_lock\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.object_lock_duration_days == 2
    error_message = "generic_choice_days (create) : payload.object_lock_duration_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.storage_class == "standard"
    error_message = "generic_choice_days (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_days"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].name != ""
    error_message = "generic_choice_years (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.enable_versioning == true
    error_message = "generic_choice_years (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.immutability_choice == "object_lock"
    error_message = "generic_choice_years (create) : payload.immutability_choice attendu \"object_lock\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.object_lock_duration_years == 2
    error_message = "generic_choice_years (create) : payload.object_lock_duration_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.storage_class == "standard"
    error_message = "generic_choice_years (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["generic_choice_years"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].name != ""
    error_message = "inferred_years (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.enable_versioning == true
    error_message = "inferred_years (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.object_lock_duration_years == 2
    error_message = "inferred_years (create) : payload.object_lock_duration_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.storage_class == "standard"
    error_message = "inferred_years (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].name != ""
    error_message = "yearly_ignores_days (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.enable_versioning == true
    error_message = "yearly_ignores_days (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.immutability_choice == "object_lock_yearly"
    error_message = "yearly_ignores_days (create) : payload.immutability_choice attendu \"object_lock_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.object_lock_duration_days == 1
    error_message = "yearly_ignores_days (create) : payload.object_lock_duration_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.object_lock_duration_years == 1
    error_message = "yearly_ignores_days (create) : payload.object_lock_duration_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.storage_class == "standard"
    error_message = "yearly_ignores_days (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_ignores_days"].payload.storage_class, null))}."
  }
}

# Attendu :
#   daily : update -> ACCEPTÉ
run "update_daily_duration" {
  variables {
    buckets = {
      daily                = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock_daily" }
      yearly               = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_yearly" }
      generic_choice_days  = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock" }
      generic_choice_years = { enable_versioning = true, object_lock_duration_years = 2, immutability_choice = "object_lock" }
      inferred_years       = { enable_versioning = true, object_lock_duration_years = 2 }
      yearly_ignores_days  = { enable_versioning = true, object_lock_duration_years = 1, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name != ""
    error_message = "daily (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name == run.create.bucket_names["daily"]
    error_message = "daily (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning == true
    error_message = "daily (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice == "object_lock_daily"
    error_message = "daily (update) : payload.immutability_choice attendu \"object_lock_daily\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_days == 2
    error_message = "daily (update) : payload.object_lock_duration_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class == "standard"
    error_message = "daily (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class, null))}."
  }
}

# Attendu :
#   yearly : update -> ACCEPTÉ
run "update_yearly_ceiling" {
  variables {
    buckets = {
      daily                = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock_daily" }
      yearly               = { enable_versioning = true, object_lock_duration_years = 5, immutability_choice = "object_lock_yearly" }
      generic_choice_days  = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock" }
      generic_choice_years = { enable_versioning = true, object_lock_duration_years = 2, immutability_choice = "object_lock" }
      inferred_years       = { enable_versioning = true, object_lock_duration_years = 2 }
      yearly_ignores_days  = { enable_versioning = true, object_lock_duration_years = 1, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].name != ""
    error_message = "yearly (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].name == run.create.bucket_names["yearly"]
    error_message = "yearly (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.enable_versioning == true
    error_message = "yearly (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.immutability_choice == "object_lock_yearly"
    error_message = "yearly (update) : payload.immutability_choice attendu \"object_lock_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.object_lock_duration_years == 5
    error_message = "yearly (update) : payload.object_lock_duration_years attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.storage_class == "standard"
    error_message = "yearly (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly"].payload.storage_class, null))}."
  }
}

# Attendu :
#   daily : update -> ACCEPTÉ
run "update_daily_to_years" {
  variables {
    buckets = {
      daily                = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_yearly" }
      yearly               = { enable_versioning = true, object_lock_duration_years = 5, immutability_choice = "object_lock_yearly" }
      generic_choice_days  = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock" }
      generic_choice_years = { enable_versioning = true, object_lock_duration_years = 2, immutability_choice = "object_lock" }
      inferred_years       = { enable_versioning = true, object_lock_duration_years = 2 }
      yearly_ignores_days  = { enable_versioning = true, object_lock_duration_years = 1, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name != ""
    error_message = "daily (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].name == run.create.bucket_names["daily"]
    error_message = "daily (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning == true
    error_message = "daily (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice == "object_lock_yearly"
    error_message = "daily (update) : payload.immutability_choice attendu \"object_lock_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_years == 1
    error_message = "daily (update) : payload.object_lock_duration_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.object_lock_duration_years, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class == "standard"
    error_message = "daily (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily"].payload.storage_class, null))}."
  }
}

# Attendu :
#   inferred_years : update -> ACCEPTÉ
run "update_inferred_to_days" {
  variables {
    buckets = {
      daily                = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_yearly" }
      yearly               = { enable_versioning = true, object_lock_duration_years = 5, immutability_choice = "object_lock_yearly" }
      generic_choice_days  = { enable_versioning = true, object_lock_duration_days = 2, immutability_choice = "object_lock" }
      generic_choice_years = { enable_versioning = true, object_lock_duration_years = 2, immutability_choice = "object_lock" }
      inferred_years       = { enable_versioning = true, object_lock_duration_days = 3 }
      yearly_ignores_days  = { enable_versioning = true, object_lock_duration_years = 1, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].name != ""
    error_message = "inferred_years (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].name == run.create.bucket_names["inferred_years"]
    error_message = "inferred_years (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.enable_versioning == true
    error_message = "inferred_years (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.object_lock_duration_days == 3
    error_message = "inferred_years (update) : payload.object_lock_duration_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.object_lock_duration_days, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.storage_class == "standard"
    error_message = "inferred_years (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["inferred_years"].payload.storage_class, null))}."
  }
}
