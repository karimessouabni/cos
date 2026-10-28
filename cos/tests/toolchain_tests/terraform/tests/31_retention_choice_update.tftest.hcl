# Incident prod : un bucket créé sans `immutability_choice` (ancien client,
# rétention seule), puis un update fait avec un provider qui envoie désormais
# `immutability_choice`, déclaré `updatable`. Tant que le DAG update ne le
# déclarait pas, l'orchestrateur refusait la demande
# ("DagParameterNotInSchema") : aucun update de ces buckets ne passait.
#
# Un seul bucket : create en rétention sans choix, puis update des permissions
# avec le choix ajouté au payload. L'update doit passer sans recréer le bucket.

variables {
  scenario = "retention-choice-update"
}

run "create_without_choice" {
  variables {
    buckets = {
      bucket = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["bucket"].name != ""
    error_message = "Le bucket n'a pas été créé."
  }
}

run "update_adds_the_choice" {
  variables {
    buckets = {
      bucket = { immutability_choice = "retention_daily", enable_custom_permissions = true, retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } }
    }
  }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["bucket"].name == run.create_without_choice.bucket_names["bucket"]
    error_message = "L'update qui ajoute immutability_choice a recréé le bucket."
  }
}
