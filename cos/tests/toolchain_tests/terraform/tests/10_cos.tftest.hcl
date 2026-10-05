# Instance COS seule : création, puis destruction automatique en fin de fichier.

variables {
  scenario     = "cos"
  cos_instance = "" # seul scénario qui crée (et détruit) une instance COS
}

run "create_cos" {
  assert {
    condition     = orchestrator_subscription_cos_v1.cos[0].name != "" && orchestrator_subscription_cos_v1.cos[0].id != ""
    error_message = "La souscription COS n'a pas de name / id."
  }
}
