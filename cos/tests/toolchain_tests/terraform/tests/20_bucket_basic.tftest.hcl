# Cycle de vie d'un bucket standard : create -> update (versioning, permissions)
# -> destroy automatique (bucket puis cos, ordre inverse des run).

run "cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} bucket basic"
  }
}

run "create_bucket" {
  module { source = "./modules/bucket" }
  variables {
    environment   = var.environment
    realm         = var.realm
    apcode        = var.apcode
    tier          = var.tier
    description   = "${var.prefix} bucket basic"
    cos_instance  = run.cos.name
    storage_class = "standard"
  }

  assert {
    condition     = output.name != ""
    error_message = "Bucket non créé."
  }
  assert {
    condition     = output.payload.storage_class == "standard"
    error_message = "storage_class attendue : standard."
  }
}

run "update_enable_versioning" {
  module { source = "./modules/bucket" }
  variables {
    environment       = var.environment
    realm             = var.realm
    apcode            = var.apcode
    tier              = var.tier
    description       = "${var.prefix} bucket basic"
    cos_instance      = run.cos.name
    storage_class     = "standard"
    enable_versioning = true
  }

  assert {
    condition     = output.name == run.create_bucket.name
    error_message = "L'update a recréé le bucket au lieu de le modifier en place."
  }
  assert {
    condition     = output.payload.enable_versioning == true
    error_message = "enable_versioning non pris en compte."
  }
}

run "update_custom_permissions" {
  module { source = "./modules/bucket" }
  variables {
    environment               = var.environment
    realm                     = var.realm
    apcode                    = var.apcode
    tier                      = var.tier
    description               = "${var.prefix} bucket basic"
    cos_instance              = run.cos.name
    storage_class             = "standard"
    enable_versioning         = true
    enable_custom_permissions = true
  }

  assert {
    condition     = output.name == run.create_bucket.name
    error_message = "L'update a recréé le bucket au lieu de le modifier en place."
  }
}
