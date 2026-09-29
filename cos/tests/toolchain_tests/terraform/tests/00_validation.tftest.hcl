# Garde-fous de var.buckets : plan seulement, rien n'est créé.
# Chaque run doit échouer sur la variable indiquée (expect_failures).

variables {
  scenario = "validation"
}

run "retention_mixed_units_is_refused" {
  command = plan
  variables {
    buckets = { b = { retention = { default_days = 30, maximum_years = 2 } } }
  }
  expect_failures = [var.buckets]
}

run "retention_bounds_are_checked" {
  command = plan
  variables {
    buckets = { b = { retention = { minimum_days = 10, default_days = 5, maximum_days = 30 } } }
  }
  expect_failures = [var.buckets]
}

run "retention_above_five_years_is_refused" {
  command = plan
  variables {
    buckets = { b = { retention = { default_years = 1, maximum_years = 6 } } }
  }
  expect_failures = [var.buckets]
}

run "unknown_storage_class_is_refused" {
  command = plan
  variables {
    buckets = { b = { storage_class = "glacier" } }
  }
  expect_failures = [var.buckets]
}
