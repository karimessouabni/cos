# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus
#
# Sans versioning, sans rétention de sauvegarde, vault inconnu, nom de vault
# vide, avec une rétention ; puis en update : désactiver la sauvegarde d'un
# bucket sans versioning, et rattacher un vault inconnu.

variables {
  scenario   = "backup rules"
  with_vault = true
}

# Attendu :
#   plain : create -> ACCEPTÉ
#   versioned : create -> ACCEPTÉ
run "create_plain_buckets" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["plain"] == null || output.bucket_status["plain"] != "DECLINED"
    error_message = "plain (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["plain"]}"
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["plain"].name != ""
    error_message = "plain (create) : souscription sans name."
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["plain"].payload.storage_class == "standard"
    error_message = "plain (create) : payload.storage_class attendu \"standard\", relu ${jsonencode(try(orchestrator_subscription_cosbucket_v1.bucket["plain"].payload.storage_class, null))}."
  }
  assert {
    condition     = output.bucket_status["versioned"] == null || output.bucket_status["versioned"] != "DECLINED"
    error_message = "versioned (create) : refusé alors que le DAG l'accepte : ${output.bucket_status_reason["versioned"]}"
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
#   r : create -> REFUSÉ (service) « Versioning should be enabled to enable bucket backup. »
run "without_versioning" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
      r         = { enable_versioning = false, backup_retention_days = 7 }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Versioning should be enabled to enable bucket backup.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Versioning should be enabled to enable bucket backup\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Bucket backup specifications are not fully set. »
run "without_retention_days" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
      r         = { enable_versioning = true, backup_enabled = true }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Bucket backup specifications are not fully set.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Bucket backup specifications are not fully set\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (dag) « The Backup Vault doesn't exist for the name : vault-inconnu-toolchain »
run "unknown_vault" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
      r         = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "vault-inconnu-toolchain" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (The Backup Vault doesn't exist for the name : vault-inconnu-toolchain) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("The Backup Vault doesn't exist for the name : vault-inconnu-toolchain", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (dag) « The Backup Vault name is required to enable bucket backup »
run "empty_vault_name" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
      r         = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (The Backup Vault name is required to enable bucket backup) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("The Backup Vault name is required to enable bucket backup", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and versioning are not compatible. We cannot activate both of them simultaneously. | Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled. »
run "with_retention_and_versioning" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true }
      r         = { enable_versioning = true, backup_retention_days = 7, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention and versioning are not compatible. We cannot activate both of them simultaneously. | Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention and versioning are not compatible\\. We cannot activate both of them simultaneously\\. \\| Retention and bucket backup are not compatible\\. We cannot activate backup when retention is enabled\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   plain : update -> REFUSÉ (service) « Versioning should be enabled to disable bucket backup. »
run "disable_backup_without_versioning" {
  variables {
    buckets = {
      plain     = { backup_enabled = false }
      versioned = { enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["plain"] == "DECLINED"
    error_message = "plain (update) : aurait dû être refusé (Versioning should be enabled to disable bucket backup.) ; status = ${jsonencode(output.bucket_status["plain"])}."
  }
  assert {
    condition     = can(regex("Versioning should be enabled to disable bucket backup\\.", output.bucket_status_reason["plain"]))
    error_message = "plain (update) : motif inattendu : ${output.bucket_status_reason["plain"]}"
  }
}

# Attendu :
#   versioned : update -> REFUSÉ (dag) « No Backup Vault exist with the name : vault-inconnu-toolchain »
run "update_with_unknown_vault" {
  variables {
    buckets = {
      plain     = {}
      versioned = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "vault-inconnu-toolchain" }
    }
  }

  assert {
    condition     = output.bucket_status["versioned"] == "DECLINED"
    error_message = "versioned (update) : aurait dû être refusé (No Backup Vault exist with the name : vault-inconnu-toolchain) ; status = ${jsonencode(output.bucket_status["versioned"])}."
  }
  assert {
    condition     = can(regex("No Backup Vault exist with the name : vault-inconnu-toolchain", output.bucket_status_reason["versioned"]))
    error_message = "versioned (update) : motif inattendu : ${output.bucket_status_reason["versioned"]}"
  }
}
