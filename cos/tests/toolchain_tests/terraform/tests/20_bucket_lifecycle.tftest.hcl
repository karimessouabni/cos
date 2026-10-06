# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Cycle de vie d'un bucket standard
#
# Création de trois buckets sans immutabilité, mises à jour en place
# (versioning, permissions, retour en arrière du versioning), puis suppression
# d'un seul bucket pendant que les autres restent.

variables {
  scenario = "bucket lifecycle"
}

# Attendu :
#   basic : create -> ACCEPTÉ
#   permissions : create -> ACCEPTÉ
#   versioned : create -> ACCEPTÉ
run "create" {
  variables {
    buckets = {
      basic       = {}
      permissions = { enable_custom_permissions = true }
      versioned   = { enable_versioning = true }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name != ""
    error_message = "basic (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class == "standard"
    error_message = "basic (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["permissions"].name != ""
    error_message = "permissions (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["permissions"].payload.enable_custom_permissions == true
    error_message = "permissions (create) : payload.enable_custom_permissions attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["permissions"].payload.enable_custom_permissions, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["permissions"].payload.storage_class == "standard"
    error_message = "permissions (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["permissions"].payload.storage_class, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].name != ""
    error_message = "versioned (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning == true
    error_message = "versioned (create) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class == "standard"
    error_message = "versioned (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class, null))}."
  }
}

# Attendu :
#   basic : update -> ACCEPTÉ
run "update_enable_versioning" {
  variables {
    buckets = {
      basic       = { enable_versioning = true }
      permissions = { enable_custom_permissions = true }
      versioned   = { enable_versioning = true }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name != ""
    error_message = "basic (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create.bucket_names["basic"]
    error_message = "basic (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_versioning == true
    error_message = "basic (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class == "standard"
    error_message = "basic (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class, null))}."
  }
}

# Attendu :
#   basic : update -> ACCEPTÉ
run "update_custom_permissions" {
  variables {
    buckets = {
      basic       = { enable_versioning = true, enable_custom_permissions = true }
      permissions = { enable_custom_permissions = true }
      versioned   = { enable_versioning = true }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name != ""
    error_message = "basic (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create.bucket_names["basic"]
    error_message = "basic (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_versioning == true
    error_message = "basic (update) : payload.enable_versioning attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_custom_permissions == true
    error_message = "basic (update) : payload.enable_custom_permissions attendu true, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.enable_custom_permissions, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class == "standard"
    error_message = "basic (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class, null))}."
  }
}

# Sans immutabilité, le versioning se désactive librement.
# Attendu :
#   versioned : update -> ACCEPTÉ
run "update_disable_versioning" {
  variables {
    buckets = {
      basic       = { enable_versioning = true, enable_custom_permissions = true }
      permissions = { enable_custom_permissions = true }
      versioned   = { enable_versioning = false }
    }
  }

  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].name != ""
    error_message = "versioned (update) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].name == run.create.bucket_names["versioned"]
    error_message = "versioned (update) : l'update a recréé la souscription au lieu de la modifier en place."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning == false
    error_message = "versioned (update) : payload.enable_versioning attendu false, relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.enable_versioning, null))}."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class == "standard"
    error_message = "versioned (update) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["versioned"].payload.storage_class, null))}."
  }
}

# cos.bucket.v1.delete sur un bucket vide ; les deux autres ne bougent pas.
# Attendu :
#   permissions : retiré -> détruit
run "delete_one_bucket" {
  variables {
    buckets = {
      basic     = { enable_versioning = true, enable_custom_permissions = true }
      versioned = { enable_versioning = false }
    }
  }

  assert {
    condition     = !contains(keys(output.bucket_names), "permissions")
    error_message = "permissions (drop) : le bucket aurait dû être détruit."
  }
}
