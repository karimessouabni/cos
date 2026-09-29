# Object lock : création avec durée 1 jour et versioning, update de la durée.

run "cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} immutability"
  }
}

run "create_object_lock" {
  module { source = "./modules/bucket" }
  variables {
    environment               = var.environment
    realm                     = var.realm
    apcode                    = var.apcode
    tier                      = var.tier
    description               = "${var.prefix} object lock"
    cos_instance              = run.cos.name
    enable_versioning         = true
    immutability_choice       = "object_lock_daily"
    object_lock_duration_days = 1
  }
  assert {
    condition     = output.payload.object_lock_duration_days == 1
    error_message = "object_lock_duration_days attendu : 1."
  }
}

run "update_object_lock_duration" {
  module { source = "./modules/bucket" }
  variables {
    environment               = var.environment
    realm                     = var.realm
    apcode                    = var.apcode
    tier                      = var.tier
    description               = "${var.prefix} object lock"
    cos_instance              = run.cos.name
    enable_versioning         = true
    immutability_choice       = "object_lock_daily"
    object_lock_duration_days = 2
  }
  assert {
    condition     = output.name == run.create_object_lock.name
    error_message = "L'update d'object lock a recréé le bucket."
  }
  assert {
    condition     = output.payload.object_lock_duration_days == 2
    error_message = "object_lock_duration_days attendu : 2."
  }
}
