# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention (ADR 0001) : jours, années, format historique
#
# Créations dans chaque unité et avec un choix explicite ; bornes fournies avec
# retention_enabled = false (bucket créé sans rétention) ; mises à jour des
# bornes, changement d'unité, borne partielle (les autres sont relues en base).

variables {
  scenario = "retention"
}

# Attendu :
#   days : create -> ACCEPTÉ
#   years : create -> ACCEPTÉ
#   legacy : create -> ACCEPTÉ
#   daily_choice : create -> ACCEPTÉ
#   yearly_choice : create -> ACCEPTÉ
#   disabled_flag : create -> ACCEPTÉ
run "create" {
  variables {
    buckets = {
      days          = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      years         = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
      legacy        = { retention = { minimum = 1, default = 2, maximum = 3 } }
      daily_choice  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_daily" }
      yearly_choice = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_yearly" }
      disabled_flag = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3, retention_enabled = false } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name != ""
    error_message = "days (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class == "standard"
    error_message = "days (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.minimum_days == 1
    error_message = "days (create) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.default_days == 2
    error_message = "days (create) : payload.retention.default_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.maximum_days == 3
    error_message = "days (create) : payload.retention.maximum_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].name != ""
    error_message = "years (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.storage_class == "standard"
    error_message = "years (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.minimum_years == 1
    error_message = "years (create) : payload.retention.minimum_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.default_years == 2
    error_message = "years (create) : payload.retention.default_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.maximum_years == 5
    error_message = "years (create) : payload.retention.maximum_years attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].name != ""
    error_message = "legacy (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.storage_class == "standard"
    error_message = "legacy (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.minimum == 1
    error_message = "legacy (create) : payload.retention.minimum attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.default == 2
    error_message = "legacy (create) : payload.retention.default attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.maximum == 3
    error_message = "legacy (create) : payload.retention.maximum attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].name != ""
    error_message = "daily_choice (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.immutability_choice == "retention_daily"
    error_message = "daily_choice (create) : payload.immutability_choice attendu \"retention_daily\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.storage_class == "standard"
    error_message = "daily_choice (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention.minimum_days == 1
    error_message = "daily_choice (create) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention.default_days == 2
    error_message = "daily_choice (create) : payload.retention.default_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention.maximum_days == 3
    error_message = "daily_choice (create) : payload.retention.maximum_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["daily_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].name != ""
    error_message = "yearly_choice (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.immutability_choice == "retention_yearly"
    error_message = "yearly_choice (create) : payload.immutability_choice attendu \"retention_yearly\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.immutability_choice, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.storage_class == "standard"
    error_message = "yearly_choice (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention.minimum_years == 1
    error_message = "yearly_choice (create) : payload.retention.minimum_years attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention.default_years == 2
    error_message = "yearly_choice (create) : payload.retention.default_years attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention.maximum_years == 3
    error_message = "yearly_choice (create) : payload.retention.maximum_years attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["yearly_choice"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].name != ""
    error_message = "disabled_flag (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.storage_class == "standard"
    error_message = "disabled_flag (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention.minimum_days == 1
    error_message = "disabled_flag (create) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention.default_days == 2
    error_message = "disabled_flag (create) : payload.retention.default_days attendu 2, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention.maximum_days == 3
    error_message = "disabled_flag (create) : payload.retention.maximum_days attendu 3, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention.retention_enabled == false
    error_message = "disabled_flag (create) : payload.retention.retention_enabled attendu false, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["disabled_flag"].payload.retention, null))}."
  }
}

# Attendu :
#   days : update -> ACCEPTÉ
run "update_days_bounds" {
  variables {
    buckets = {
      days          = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      years         = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
      legacy        = { retention = { minimum = 1, default = 2, maximum = 3 } }
      daily_choice  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_daily" }
      yearly_choice = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_yearly" }
      disabled_flag = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3, retention_enabled = false } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name != ""
    error_message = "days (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name == run.create.bucket_names["days"]
    error_message = "days (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class == "standard"
    error_message = "days (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.minimum_days == 1
    error_message = "days (update) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.default_days == 5
    error_message = "days (update) : payload.retention.default_days attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.maximum_days == 10
    error_message = "days (update) : payload.retention.maximum_days attendu 10, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
}

# Les bornes en base sont en jours : on peut repasser en jours.
# Attendu :
#   years : update -> ACCEPTÉ
run "update_years_to_days" {
  variables {
    buckets = {
      days          = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      years         = { retention = { minimum_days = 1, default_days = 400, maximum_days = 800 } }
      legacy        = { retention = { minimum = 1, default = 2, maximum = 3 } }
      daily_choice  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_daily" }
      yearly_choice = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_yearly" }
      disabled_flag = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3, retention_enabled = false } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].name != ""
    error_message = "years (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].name == run.create.bucket_names["years"]
    error_message = "years (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.storage_class == "standard"
    error_message = "years (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.minimum_days == 1
    error_message = "years (update) : payload.retention.minimum_days attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.default_days == 400
    error_message = "years (update) : payload.retention.default_days attendu 400, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention.maximum_days == 800
    error_message = "years (update) : payload.retention.maximum_days attendu 800, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["years"].payload.retention, null))}."
  }
}

# Attendu :
#   legacy : update -> ACCEPTÉ
run "update_legacy_bounds" {
  variables {
    buckets = {
      days          = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      years         = { retention = { minimum_days = 1, default_days = 400, maximum_days = 800 } }
      legacy        = { retention = { minimum = 1, default = 5, maximum = 10 } }
      daily_choice  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_daily" }
      yearly_choice = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_yearly" }
      disabled_flag = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3, retention_enabled = false } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].name != ""
    error_message = "legacy (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].name == run.create.bucket_names["legacy"]
    error_message = "legacy (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.storage_class == "standard"
    error_message = "legacy (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.minimum == 1
    error_message = "legacy (update) : payload.retention.minimum attendu 1, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.default == 5
    error_message = "legacy (update) : payload.retention.default attendu 5, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention.maximum == 10
    error_message = "legacy (update) : payload.retention.maximum attendu 10, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["legacy"].payload.retention, null))}."
  }
}

# Seule la borne envoyée change, minimum et maximum sont relus en base.
# Attendu :
#   days : update -> ACCEPTÉ
run "update_partial_default" {
  variables {
    buckets = {
      days          = { retention = { default_days = 7 } }
      years         = { retention = { minimum_days = 1, default_days = 400, maximum_days = 800 } }
      legacy        = { retention = { minimum = 1, default = 5, maximum = 10 } }
      daily_choice  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_daily" }
      yearly_choice = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_yearly" }
      disabled_flag = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3, retention_enabled = false } }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name != ""
    error_message = "days (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name == run.create.bucket_names["days"]
    error_message = "days (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class == "standard"
    error_message = "days (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention.default_days == 7
    error_message = "days (update) : payload.retention.default_days attendu 7, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["days"].payload.retention, null))}."
  }
}
