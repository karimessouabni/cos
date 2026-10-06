# Cas d'échec de l'immutabilité : chaque run envoie UN payload invalide et
# vérifie que l'orchestrateur le refuse (status DECLINED) avec le motif du DAG.
# Les règles sont celles des DAGs (BucketRetention, apply_choice_unit,
# compute_bucket_retention) : rien n'est revérifié côté Terraform, c'est bien
# le produit qui est testé. Un bucket accepté ici est une régression.
#
# Chaque run remplace la map `buckets` : le bucket du run précédent est
# détruit (s'il existe) avant le suivant.

variables {
  scenario = "immutability failures"
}

# --- Rétention : plafond de 5 ans ---------------------------------------------

run "retention_days_over_five_years" {
  variables {
    buckets = { r = { retention = { minimum_days = 1, default_days = 1900, maximum_days = 1900 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "1900 jours (> 5 ans) aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)5 years|cannot be superior", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_years_over_five" {
  variables {
    buckets = { r = { retention = { minimum_years = 1, default_years = 6, maximum_years = 6 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "6 ans aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)5 years", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_legacy_over_five_years" {
  variables {
    buckets = { r = { retention = { minimum = 1, default = 1900, maximum = 1900 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Format historique 1900 jours (> 5 ans) aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

# --- Rétention : unités mélangées ---------------------------------------------

run "retention_days_and_years_mixed" {
  variables {
    buckets = { r = { retention = { minimum_days = 1, default_days = 2, maximum_years = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Jours et années mélangés auraient dû être refusés ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)not a mix|either in days|same unit", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_legacy_and_new_format_mixed" {
  variables {
    buckets = { r = { retention = { minimum = 1, default = 2, maximum_days = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Format historique + champs suffixés auraient dû être refusés ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)legacy|not both", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_legacy_and_years_mixed" {
  variables {
    buckets = { r = { retention = { default = 2, maximum_years = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Format historique + années auraient dû être refusés ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

# --- Rétention : bornes incohérentes ------------------------------------------

run "retention_minimum_above_maximum" {
  variables {
    buckets = { r = { retention = { minimum_days = 10, default_days = 5, maximum_days = 5 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "minimum > maximum aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)minimum", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_default_outside_bounds" {
  variables {
    buckets = { r = { retention = { minimum_days = 1, default_days = 20, maximum_days = 10 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "default > maximum aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

run "retention_zero_is_refused" {
  variables {
    buckets = { r = { retention = { minimum_days = 0, default_days = 1, maximum_days = 2 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Une borne à 0 aurait dû être refusée ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

run "retention_incomplete_bounds" {
  variables {
    buckets = { r = { retention = { default_days = 5 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Une rétention sans minimum ni maximum aurait dû être refusée ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)default, minimum and maximum", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# --- Rétention : choix d'unité contredit par les valeurs ----------------------

run "retention_yearly_choice_with_days_values" {
  variables {
    buckets = { r = { immutability_choice = "retention_yearly", retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "retention_yearly avec des jours aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)must be set in years", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_daily_choice_with_years_values" {
  variables {
    buckets = { r = { immutability_choice = "retention_daily", retention = { minimum_years = 1, default_years = 2, maximum_years = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "retention_daily avec des années aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)must be set in days", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "retention_choice_without_values" {
  variables {
    buckets = { r = { immutability_choice = "retention_daily" } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "retention_daily sans rétention aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)must not be empty", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

# --- Object lock : plafond de 5 ans et unités ---------------------------------

run "object_lock_days_over_five_years" {
  variables {
    buckets = { r = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 1900 } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Object lock 1900 jours (> 5 ans) aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

run "object_lock_years_over_five" {
  variables {
    buckets = { r = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_years = 6 } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Object lock 6 ans aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

run "object_lock_daily_choice_without_days" {
  variables {
    buckets = { r = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_years = 1 } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "object_lock_daily sans durée en jours aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)must be set in days", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "object_lock_yearly_choice_without_years" {
  variables {
    buckets = { r = { enable_versioning = true, immutability_choice = "object_lock_yearly", object_lock_duration_days = 1 } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "object_lock_yearly sans durée en années aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)must be set in years", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}

run "object_lock_zero_days" {
  variables {
    buckets = { r = { enable_versioning = true, immutability_choice = "object_lock_daily", object_lock_duration_days = 0 } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Object lock 0 jour aurait dû être refusé ; status = ${jsonencode(output.bucket_status["r"])}."
  }
}

# --- Rétention et object lock ensemble ----------------------------------------

run "retention_and_object_lock_together" {
  variables {
    buckets = { r = { enable_versioning = true, object_lock_duration_days = 1, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } } }
  }
  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "Rétention + object lock auraient dû être refusés ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("(?i)not compatible", output.bucket_status_reason["r"]))
    error_message = "Motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}
