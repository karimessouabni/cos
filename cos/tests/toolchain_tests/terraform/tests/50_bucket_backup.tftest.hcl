# Backup vault : cos -> vault -> bucket sauvegardé, update de la rétention des
# sauvegardes. Destruction automatique en ordre inverse (bucket, vault, cos).

run "cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} backup"
  }
}

run "vault" {
  module { source = "./modules/backup_vault" }
  variables {
    environment  = var.environment
    realm        = var.realm
    apcode       = var.apcode
    description  = "${var.prefix} backup vault"
    cos_instance = run.cos.name
  }
  assert {
    condition     = output.id != ""
    error_message = "Backup vault non créé."
  }
}

run "create_bucket_with_backup" {
  module { source = "./modules/bucket" }
  variables {
    environment       = var.environment
    realm             = var.realm
    apcode            = var.apcode
    tier              = var.tier
    description       = "${var.prefix} bucket with backup"
    cos_instance      = run.cos.name
    enable_versioning = true
    backup            = { vault_sub_id = run.vault.id, retention_days = 1 }
  }
  assert {
    condition     = output.payload.backup.backup_enabled == true && output.payload.backup.backup_vault_sub_id == run.vault.id
    error_message = "Le bucket n'est pas rattaché au backup vault."
  }
}

run "update_backup_retention" {
  module { source = "./modules/bucket" }
  variables {
    environment       = var.environment
    realm             = var.realm
    apcode            = var.apcode
    tier              = var.tier
    description       = "${var.prefix} bucket with backup"
    cos_instance      = run.cos.name
    enable_versioning = true
    backup            = { vault_sub_id = run.vault.id, retention_days = 2 }
  }
  assert {
    condition     = output.name == run.create_bucket_with_backup.name
    error_message = "L'update de backup a recréé le bucket."
  }
  assert {
    condition     = output.payload.backup.backup_retention_days == 2
    error_message = "backup_retention_days attendu : 2."
  }
}
