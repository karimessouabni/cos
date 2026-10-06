# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Rétention : refus à la création
#
# Chaque run envoie un payload de rétention invalide : plafond de 5 ans dans
# chaque format, unités mélangées, bornes incohérentes ou incomplètes, choix
# _daily/_yearly contredit par les valeurs, incompatibilités (versioning, object
# lock, sauvegarde).

variables {
  scenario   = "retention rules"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (schema) « default_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). | maximum_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). »
run "days_over_five_years" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (default_days (1900 days) cannot be superior to 5 years (1826 days, leap years included).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("default_days \\(\\d+ days\\) cannot be superior to 5 years \\(\\d+ days, leap years included\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « default_years (6 years) cannot be superior to 5 years (5 years, leap years included). | maximum_years (6 years) cannot be superior to 5 years (5 years, leap years included). »
run "years_over_five" {
  variables {
    buckets = {
      r = { retention = { minimum_years = 1, default_years = 6, maximum_years = 6 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (default_years (6 years) cannot be superior to 5 years (5 years, leap years included).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("default_years \\(6 years\\) cannot be superior to 5 years \\(5 years, leap years included\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « default_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). | maximum_days (1900 days) cannot be superior to 5 years (1826 days, leap years included). »
run "legacy_over_five_years" {
  variables {
    buckets = {
      r = { retention = { minimum = 1, default = 1900, maximum = 1900 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (default_days (1900 days) cannot be superior to 5 years (1826 days, leap years included).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("default_days \\(\\d+ days\\) cannot be superior to 5 years \\(\\d+ days, leap years included\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention must be set either in days (…_days) or in years (…_years) for all three attributes, not a mix of both. »
run "days_and_years_mixed" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_years = 3 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must be set either in days (…_days) or in years (…_years) for all three attributes, not a mix of both.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must be set either in days \\(…_days\\) or in years \\(…_years\\) for all three attributes, not a mix of both\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention must use either the legacy fields (default, minimum, maximum) or the unit-suffixed fields (default_days, maximum_days), not both. »
run "legacy_and_days_mixed" {
  variables {
    buckets = {
      r = { retention = { minimum = 1, default_days = 2, maximum_days = 3 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must use either the legacy fields (default, minimum, maximum) or the unit-suffixed fields (default_days, maximum_days), not both.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must use either the legacy fields \\(default, minimum, maximum\\) or the unit-suffixed fields \\(default_days, maximum_days\\), not both\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention must use either the legacy fields (default, minimum, maximum) or the unit-suffixed fields (maximum_years), not both. »
run "legacy_and_years_mixed" {
  variables {
    buckets = {
      r = { retention = { minimum = 1, default = 2, maximum_years = 3 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must use either the legacy fields (default, minimum, maximum) or the unit-suffixed fields (maximum_years), not both.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must use either the legacy fields \\(default, minimum, maximum\\) or the unit-suffixed fields \\(maximum_years\\), not both\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention minimum cannot be superior to maximum. | Retention default cannot be inferior to minimum. | Retention default cannot be superior to maximum. »
run "minimum_above_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 3, default_days = 2, maximum_days = 1 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention minimum cannot be superior to maximum.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention minimum cannot be superior to maximum\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention default cannot be inferior to minimum. »
run "default_below_minimum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 5, default_days = 2, maximum_days = 10 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention default cannot be inferior to minimum.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention default cannot be inferior to minimum\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « Retention default cannot be superior to maximum. »
run "default_above_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 20, maximum_days = 10 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention default cannot be superior to maximum.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention default cannot be superior to maximum\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « minimum_days must be superior to 0. »
run "zero_bound" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 0, default_days = 1, maximum_days = 2 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (minimum_days must be superior to 0.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("minimum_days must be superior to 0\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « minimum_years must be superior to 0. »
run "negative_bound" {
  variables {
    buckets = {
      r = { retention = { minimum_years = -1, default_years = 1, maximum_years = 2 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (minimum_years must be superior to 0.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("minimum_years must be superior to 0\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days (…_days) or in years (…_years). »
run "only_default" {
  variables {
    buckets = {
      r = { retention = { default_days = 2 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days (…_days) or in years (…_years).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention configuration is not valid\\. You must set default, minimum and maximum in the same unit, either in days \\(…_days\\) or in years \\(…_years\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days (…_days) or in years (…_years). »
run "only_minimum_and_maximum" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, maximum_days = 3 } }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention configuration is not valid. You must set default, minimum and maximum in the same unit, either in days (…_days) or in years (…_years).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention configuration is not valid\\. You must set default, minimum and maximum in the same unit, either in days \\(…_days\\) or in years \\(…_years\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must be set in years (default_years, minimum_years, maximum_years) for choice retention_yearly. »
run "yearly_choice_with_days" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, immutability_choice = "retention_yearly" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must be set in years (default_years, minimum_years, maximum_years) for choice retention_yearly.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must be set in years \\(default_years, minimum_years, maximum_years\\) for choice retention_yearly\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must be set in days (default_days, minimum_days, maximum_days) for choice retention_daily. »
run "daily_choice_with_years" {
  variables {
    buckets = {
      r = { retention = { minimum_years = 1, default_years = 2, maximum_years = 3 }, immutability_choice = "retention_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must be set in days (default_days, minimum_days, maximum_days) for choice retention_daily.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must be set in days \\(default_days, minimum_days, maximum_days\\) for choice retention_daily\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must not be empty. »
run "choice_without_values" {
  variables {
    buckets = {
      r = { immutability_choice = "retention" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must not be empty.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must not be empty\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention must not be empty for choice retention_daily. »
run "daily_choice_without_values" {
  variables {
    buckets = {
      r = { immutability_choice = "retention_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention must not be empty for choice retention_daily.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention must not be empty for choice retention_daily\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and versioning are not compatible. We cannot activate both of them simultaneously. »
run "with_versioning" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention and versioning are not compatible. We cannot activate both of them simultaneously.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention and versioning are not compatible\\. We cannot activate both of them simultaneously\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and object Lock are not compatible. We cannot activate both of them simultaneously. »
run "with_object_lock" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, object_lock_duration_days = 1 }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention and object Lock are not compatible. We cannot activate both of them simultaneously.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention and object Lock are not compatible\\. We cannot activate both of them simultaneously\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and object Lock are not compatible. We cannot activate both of them simultaneously. »
run "with_object_lock_and_versioning" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, object_lock_duration_days = 1, enable_versioning = true }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention and object Lock are not compatible. We cannot activate both of them simultaneously.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention and object Lock are not compatible\\. We cannot activate both of them simultaneously\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled. »
run "with_backup" {
  variables {
    buckets = {
      r = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 }, backup_retention_days = 7 }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Retention and bucket backup are not compatible. We cannot activate backup when retention is enabled.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Retention and bucket backup are not compatible\\. We cannot activate backup when retention is enabled\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}
