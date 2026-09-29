# Garde-fous du module bucket : plan seulement, rien n'est créé.
# Chaque run doit échouer sur la variable indiquée (expect_failures).

run "retention_mixed_units_is_refused" {
  command = plan
  module { source = "./modules/bucket" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} validation"
    cos_instance = "co21000001"
    retention    = { default_days = 30, maximum_years = 2 }
  }
  expect_failures = [var.retention]
}

run "retention_bounds_are_checked" {
  command = plan
  module { source = "./modules/bucket" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} validation"
    cos_instance = "co21000001"
    retention    = { minimum_days = 10, default_days = 5, maximum_days = 30 }
  }
  expect_failures = [var.retention]
}

run "retention_above_five_years_is_refused" {
  command = plan
  module { source = "./modules/bucket" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} validation"
    cos_instance = "co21000001"
    retention    = { default_years = 1, maximum_years = 6 }
  }
  expect_failures = [var.retention]
}

run "unknown_storage_class_is_refused" {
  command = plan
  module { source = "./modules/bucket" }
  variables {
    environment   = var.environment
    realm         = var.realm
    apcode        = var.apcode
    tier          = var.tier
    description   = "${var.prefix} validation"
    cos_instance  = "co21000001"
    storage_class = "glacier"
  }
  expect_failures = [var.storage_class]
}
