# Rétention (ADR 0001) : un bucket en jours et un en années, créés ensemble,
# puis mise à jour des bornes du bucket en jours (l'autre ne doit pas bouger).
# Le "second plan sans changement" se vérifie avec
# toolchain_env.py --run plan -- -detailed-exitcode (code 2 = drift).

run "cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} retention"
  }
}

run "create_days_and_years" {
  module { source = "./modules/buckets" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} retention"
    cos_instance = run.cos.name
    buckets = {
      days  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      years = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
    }
  }

  assert {
    condition     = output.payloads.days.retention.default_days == 2 && output.payloads.years.retention.default_years == 2
    error_message = "Bornes de rétention envoyées incorrectes."
  }
  assert {
    condition     = !contains(keys(output.payloads.years.retention), "default_days")
    error_message = "Le bucket en années ne doit porter aucune borne en jours."
  }
}

run "update_days_bounds" {
  module { source = "./modules/buckets" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    tier         = var.tier
    description  = "${var.prefix} retention"
    cos_instance = run.cos.name
    buckets = {
      days  = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      years = { retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
    }
  }

  assert {
    condition     = output.names == run.create_days_and_years.names
    error_message = "L'update de rétention a recréé un bucket."
  }
  assert {
    condition     = output.payloads.days.retention.default_days == 5 && output.payloads.days.retention.maximum_days == 10
    error_message = "Bornes de rétention non mises à jour."
  }
}
