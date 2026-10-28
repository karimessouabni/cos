# Incident prod : `immutability_choice` est déclaré `updatable` à la création,
# donc le provider orchestrator le renvoie dans chaque update. Tant que le DAG
# update ne le déclarait pas, l'orchestrateur refusait la demande
# ("DagParameterNotInSchema") : aucun update de bucket ne passait.
#
# Un seul bucket en rétention, créé avec son choix, puis un update des
# permissions où le choix est renvoyé tel quel. L'update doit passer sans
# recréer le bucket.

variables {
  scenario = "retention-choice-update"
}

run "create" {
  variables {
    buckets = {
      bucket = { immutability_choice = "retention_daily", retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["bucket"].name != ""
    error_message = "Le bucket n'a pas été créé."
  }
}

run "update_with_same_choice" {
  variables {
    buckets = {
      bucket = { immutability_choice = "retention_daily", enable_custom_permissions = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["bucket"].name == run.create.bucket_names["bucket"]
    error_message = "L'update avec immutability_choice a recréé le bucket."
  }
}
