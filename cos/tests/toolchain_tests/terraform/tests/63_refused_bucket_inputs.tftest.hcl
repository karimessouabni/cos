# Tous les refus de validation d'un bucket en un seul fichier.
#
# Un seul run : chaque clé de `buckets` est un cas invalide, créé sur l'instance
# COS existante du tfvars (cos_instance). Les buckets sont indépendants, tofu les
# applique tous et rapporte une erreur « Cannot create subscription » par bucket
# refusé, avec le motif du DAG. toolchain_env.py --run test juge ensuite chaque
# clé contre tests/expected_failures.json et liste les motifs trouvés / manquants.
#
# Un cas accepté (pas d'erreur sur sa clé) est une régression : le DAG accepte ce
# qu'il doit refuser ; le bucket créé est détruit en fin de fichier par tofu.
#
# Lancer seul :
#   python toolchain_env.py --env int --run test -- -filter=tests/63_refused_bucket_inputs.tftest.hcl
#
# Pour ajouter un cas : une clé ici, la même clé dans expected_failures.json.

variables {
  scenario = "refused bucket inputs"
}

run "refused" {
  variables {
    buckets = {
      # --- Rétention : schéma BucketRetention (cos_service/schemas/bucket_retention.py)
      retention_mixed_units         = { retention = { minimum_days = 1, default_years = 2, maximum_days = 3 } }
      retention_zero                = { retention = { minimum_days = 0, default_days = 2, maximum_days = 3 } }
      retention_negative            = { retention = { minimum_days = 1, default_days = -2, maximum_days = 3 } }
      retention_days_over_limit     = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } }
      retention_years_over_limit    = { retention = { minimum_years = 1, default_years = 6, maximum_years = 6 } }
      retention_legacy_over_limit   = { retention = { minimum = 1, default = 1900, maximum = 1900 } }
      retention_min_over_max        = { retention = { minimum_days = 10, default_days = 10, maximum_days = 5 } }
      retention_default_under_min   = { retention = { minimum_days = 5, default_days = 2, maximum_days = 10 } }
      retention_default_over_max    = { retention = { minimum_days = 1, default_days = 20, maximum_days = 10 } }
      retention_legacy_and_suffixed = { retention = { default = 2, minimum_days = 1, maximum_days = 3 } }

      # --- Rétention : service d'immutabilité (cos_service/services/immutability_service.py)
      retention_incomplete            = { retention = { default_days = 2 } }
      retention_with_versioning       = { enable_versioning = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retention_with_object_lock      = { object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retention_choice_without_values = { immutability_choice = "retention" }
      retention_choice_disabled       = { immutability_choice = "retention", retention = { retention_enabled = false, minimum_days = 1, default_days = 2, maximum_days = 3 } }
      retention_daily_without_values  = { immutability_choice = "retention_daily" }
      retention_daily_in_years        = { immutability_choice = "retention_daily", retention = { minimum_years = 1, default_years = 2, maximum_years = 3 } }
      retention_yearly_in_days        = { immutability_choice = "retention_yearly", retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }

      # --- Object lock
      object_lock_without_versioning   = { object_lock_duration_days = 1 }
      object_lock_both_units           = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1 }
      object_lock_zero                 = { enable_versioning = true, object_lock_duration_days = 0 }
      object_lock_days_over_limit      = { enable_versioning = true, object_lock_duration_days = 1900 }
      object_lock_years_over_limit     = { enable_versioning = true, object_lock_duration_years = 6 }
      object_lock_choice_without_value = { enable_versioning = true, immutability_choice = "object_lock" }
      object_lock_daily_in_years       = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_years = 1 }
      object_lock_yearly_in_days       = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_days = 1 }

      # --- Backup (sans backup_retention_days : aucun vault n'est créé, le DAG
      #     refuse sur le nom de vault manquant)
      backup_without_vault = { enable_versioning = true, backup_enabled = true }

      # --- Contexte et énumérations du payload (BucketCreatePayload)
      unknown_cos_instance        = { cos_instance = "co000i000000" }
      unknown_storage_class       = { storage_class = "glacier" }
      unknown_immutability_choice = { immutability_choice = "forever" }
    }
  }
}
