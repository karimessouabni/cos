# Incident prod : `immutability_choice` est déclaré `updatable` à la création,
# donc le provider orchestrator le renvoie dans chaque update. Tant que le DAG
# update ne le déclarait pas, l'orchestrateur refusait la demande
# ("DagParameterNotInSchema") : aucun update de bucket ne passait.
#
# Ici le choix est envoyé explicitement à chaque run, comme le provider le
# fait. Un update doit passer sans recréer le bucket, que le champ modifié
# soit hors immutabilité (permissions) ou dans la rétention (bornes).
# Un bucket sans immutabilité ("none") et un bucket en rétention annuelle
# couvrent les trois familles acceptées par le DAG.

variables {
  scenario = "retention-choice-update"
}

run "create" {
  variables {
    buckets = {
      none   = { immutability_choice = "none" }
      daily  = { immutability_choice = "retention_daily", retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      yearly = { immutability_choice = "retention_yearly", retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
    }
  }
  assert {
    condition     = alltrue([for b in orchestrator_subscription_cosbucket_v1.bucket : b.name != ""])
    error_message = "Un bucket n'a pas été créé."
  }
}

# Update d'un champ hors immutabilité : le choix est renvoyé tel quel.
run "update_permissions_with_same_choice" {
  variables {
    buckets = {
      none   = { immutability_choice = "none", enable_custom_permissions = true }
      daily  = { immutability_choice = "retention_daily", enable_custom_permissions = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
      yearly = { immutability_choice = "retention_yearly", enable_custom_permissions = true, retention = { minimum_years = 1, default_years = 2, maximum_years = 5 } }
    }
  }
  assert {
    condition = alltrue([
      for k, b in orchestrator_subscription_cosbucket_v1.bucket : b.name == run.create.bucket_names[k]
    ])
    error_message = "L'update des permissions avec immutability_choice a recréé un bucket."
  }
}

# Update des bornes de rétention, choix toujours renvoyé.
run "update_bounds_with_same_choice" {
  variables {
    buckets = {
      none   = { immutability_choice = "none", enable_custom_permissions = true }
      daily  = { immutability_choice = "retention_daily", enable_custom_permissions = true, retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } }
      yearly = { immutability_choice = "retention_yearly", enable_custom_permissions = true, retention = { minimum_years = 1, default_years = 3, maximum_years = 5 } }
    }
  }
  assert {
    condition = alltrue([
      for k, b in orchestrator_subscription_cosbucket_v1.bucket : b.name == run.create.bucket_names[k]
    ])
    error_message = "L'update des bornes de rétention avec immutability_choice a recréé un bucket."
  }
}
