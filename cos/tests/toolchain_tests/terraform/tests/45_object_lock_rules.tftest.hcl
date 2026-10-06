# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Object lock : refus à la création
#
# Sans versioning, deux unités, durée nulle ou négative, au-delà de 5 ans dans
# chaque unité, choix _daily/_yearly sans la durée attendue, choix générique
# sans durée.

variables {
  scenario = "object lock rules"
}

# Attendu :
#   r : create -> REFUSÉ (service) « Versioning should be enabled to enable object-lock. »
run "without_versioning" {
  variables {
    buckets = {
      r = { enable_versioning = false, object_lock_duration_days = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Versioning should be enabled to enable object-lock.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Versioning should be enabled to enable object-lock\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set either in days (object_lock_duration_days) or in years (object_lock_duration_years), not both. »
run "both_units" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1, object_lock_duration_years = 1 }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention must be set either in days (object_lock_duration_days) or in years (object_lock_duration_years), not both.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention must be set either in days \\(object_lock_duration_days\\) or in years \\(object_lock_duration_years\\), not both\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (0 days) cannot be inferior or equal to ZERO. »
run "zero_days" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 0, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention (0 days) cannot be inferior or equal to ZERO.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention \\(\\d+ days\\) cannot be inferior or equal to ZERO\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (-1 years) cannot be inferior or equal to ZERO. »
run "negative_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = -1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention (-1 years) cannot be inferior or equal to ZERO.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention \\(-1 years\\) cannot be inferior or equal to ZERO\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (1900 days) cannot be superior to 5 years (1826 days). »
run "days_over_five_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1900, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention (1900 days) cannot be superior to 5 years (1826 days).) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention \\(\\d+ days\\) cannot be superior to 5 years \\(\\d+ days\\)\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention (6 years) cannot be superior to 5 years. »
run "years_over_five" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = 6, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention (6 years) cannot be superior to 5 years.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention \\(6 years\\) cannot be superior to 5 years\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set in days (object_lock_duration_days) for choice object_lock_daily. »
run "daily_choice_without_days" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_years = 1, immutability_choice = "object_lock_daily" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention must be set in days (object_lock_duration_days) for choice object_lock_daily.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention must be set in days \\(object_lock_duration_days\\) for choice object_lock_daily\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object lock retention must be set in years (object_lock_duration_years) for choice object_lock_yearly. »
run "yearly_choice_without_years" {
  variables {
    buckets = {
      r = { enable_versioning = true, object_lock_duration_days = 1, immutability_choice = "object_lock_yearly" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object lock retention must be set in years (object_lock_duration_years) for choice object_lock_yearly.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object lock retention must be set in years \\(object_lock_duration_years\\) for choice object_lock_yearly\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (service) « Object Lock Duration Days or Years must be not empty. »
run "generic_choice_without_duration" {
  variables {
    buckets = {
      r = { enable_versioning = true, immutability_choice = "object_lock" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (Object Lock Duration Days or Years must be not empty.) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("Object Lock Duration Days or Years must be not empty\\.", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# Attendu :
#   r : create -> REFUSÉ (schema) « 'object_lock_monthly' is not a valid Immutability »
run "unknown_choice" {
  variables {
    buckets = {
      r = { enable_versioning = true, immutability_choice = "object_lock_monthly" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé ('object_lock_monthly' is not a valid Immutability) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("'object_lock_monthly' is not a valid Immutability", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}
