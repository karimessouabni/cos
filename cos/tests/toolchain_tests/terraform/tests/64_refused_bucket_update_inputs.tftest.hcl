# Tous les refus de validation d'un UPDATE de bucket en un seul fichier.
#
# Deux runs sur la même map `buckets`, une clé par cas :
#   - "create" : chaque bucket est créé dans un état VALIDE (vierge, en object
#     lock, ou en rétention) sur l'instance COS existante du tfvars ;
#   - "update" : chaque clé reçoit le payload interdit pour son état de départ.
# Les buckets sont indépendants : tofu applique tous les updates et rapporte un
# diagnostic « Cannot update subscription » par bucket refusé, avec le motif du
# DAG cos.bucket.v1.update. toolchain_env.py --run test juge ensuite chaque clé
# contre tests/expected_failures.json (le run "create" doit passer).
#
# Un cas accepté (pas d'erreur sur sa clé) est une régression. Tout est détruit
# en fin de fichier par tofu.
#
# Lancer seul :
#   python toolchain_env.py --env int --run test -- -filter=tests/64_refused_bucket_update_inputs.tftest.hcl
#
# Pour ajouter un cas : la même clé dans les deux runs et dans expected_failures.json.

variables {
  scenario = "refused bucket update inputs"
}

# --- État de départ, valide ----------------------------------------------------
run "create" {
  variables {
    buckets = {
      # bucket vierge : ni versioning, ni rétention, ni object lock
      plain_retention_mixed_units             = {}
      plain_retention_zero                    = {}
      plain_retention_over_limit              = {}
      plain_retention_incomplete              = {}
      plain_retention_with_versioning         = {}
      plain_retention_with_object_lock        = {}
      plain_object_lock_without_versioning    = {}
      plain_object_lock_both_units            = {}
      plain_object_lock_zero                  = {}
      plain_object_lock_over_limit            = {}
      plain_backup_without_vault              = {}
      plain_backup_disable_without_versioning = {}

      # bucket déjà en object lock (versioning + 1 jour)
      locked_set_retention        = { enable_versioning = true, object_lock_duration_days = 1 }
      locked_disable_versioning   = { enable_versioning = true, object_lock_duration_days = 1 }
      locked_both_units           = { enable_versioning = true, object_lock_duration_days = 1 }
      locked_zero                 = { enable_versioning = true, object_lock_duration_days = 1 }
      locked_over_limit           = { enable_versioning = true, object_lock_duration_days = 1 }
      locked_backup_without_vault = { enable_versioning = true, object_lock_duration_days = 1 }

      # bucket déjà en rétention (1 / 2 / 3 jours)
      retained_set_object_lock       = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_enable_versioning     = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_backup                = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_minimum_over_default  = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_default_equal_maximum = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_maximum_under_default = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_maximum_over_limit    = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_switch_to_years       = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_mixed_units           = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
  assert {
    condition     = alltrue([for b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Un bucket de départ n'a pas été créé : les refus d'update ne peuvent pas être testés."
  }
}

# --- Payloads interdits, un par clé --------------------------------------------
run "update" {
  variables {
    buckets = {
      # bucket vierge : les règles de création s'appliquent à l'update
      plain_retention_mixed_units             = { retention = { minimum_days = 1, default_years = 2, maximum_days = 3 } }
      plain_retention_zero                    = { retention = { minimum_days = 0, default_days = 2, maximum_days = 3 } }
      plain_retention_over_limit              = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } }
      plain_retention_incomplete              = { retention = { default_days = 2 } }
      plain_retention_with_versioning         = { enable_versioning = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      plain_retention_with_object_lock        = { object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      plain_object_lock_without_versioning    = { object_lock_duration_days = 1 }
      plain_object_lock_both_units            = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1 }
      plain_object_lock_zero                  = { enable_versioning = true, object_lock_duration_days = 0 }
      plain_object_lock_over_limit            = { enable_versioning = true, object_lock_duration_years = 6 }
      plain_backup_without_vault              = { backup_enabled = true }
      plain_backup_disable_without_versioning = { backup_enabled = false }

      # bucket en object lock : ni rétention, ni arrêt du versioning, durée bornée
      locked_set_retention        = { enable_versioning = true, object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      locked_disable_versioning   = { enable_versioning = false, object_lock_duration_days = 1 }
      locked_both_units           = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1 }
      locked_zero                 = { enable_versioning = true, object_lock_duration_days = 0 }
      locked_over_limit           = { enable_versioning = true, object_lock_duration_years = 6 }
      locked_backup_without_vault = { enable_versioning = true, object_lock_duration_days = 1, backup_enabled = true }

      # bucket en rétention : ni object lock, ni versioning, ni backup ; bornes
      # comparées à l'existant (1 / 2 / 3 jours), en jours
      retained_set_object_lock       = { object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_enable_versioning     = { enable_versioning = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_backup                = { backup_enabled = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retained_minimum_over_default  = { retention = { minimum_days = 5 } }
      retained_default_equal_maximum = { retention = { default_days = 3 } }
      retained_maximum_under_default = { retention = { maximum_days = 2 } }
      retained_maximum_over_limit    = { retention = { maximum_days = 1900 } }
      retained_switch_to_years       = { retention = { minimum_years = 1 } }
      retained_mixed_units           = { retention = { minimum_days = 1, default_years = 2 } }
    }
  }
}
