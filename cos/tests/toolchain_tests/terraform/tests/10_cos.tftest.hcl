# Instance COS seule : création, puis destruction automatique en fin de fichier.

variables {
  scenario = "cos"
}

run "create_cos" {
  assert {
    condition     = orchestrator_subscription_cos_v1.cos.name != "" && orchestrator_subscription_cos_v1.cos.id != ""
    error_message = "La souscription COS n'a pas de name / id."
  }
}
