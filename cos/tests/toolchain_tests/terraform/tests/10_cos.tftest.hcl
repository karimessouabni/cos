# Instance COS seule : création, puis destruction automatique en fin de fichier.

run "create_cos" {
  module { source = "./modules/cos" }
  variables {
    environment = var.environment
    realm       = var.realm
    apcode      = var.apcode
    tier        = var.tier
    description = "${var.prefix} cos create/delete"
  }

  assert {
    condition     = output.name != "" && output.id != ""
    error_message = "La souscription COS n'a pas de name / id."
  }
}
