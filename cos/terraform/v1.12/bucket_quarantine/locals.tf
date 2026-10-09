locals {
  # Identiques au workspace du bucket : chemin Vault de la clé API du compte workload.
  vault_namespace           = var.app_code
  vault_secret_path_account = "ibm/${var.wklapp_account_id}"

  # L'instance COS est identifiée par son GUID dans les attributs CBR.
  cos_instance_guid = element(split(":", var.cos_instance_crn), 7)

  # Règle appliquée (bucket fermé) ou désactivée (bucket ouvert le temps d'une
  # action de l'orchestrateur). Désactiver plutôt que détruire : la règle et son
  # identifiant restent, la remettre est une simple mise à jour.
  effective_enforcement_mode = var.rule_active ? var.enforcement_mode : "disabled"
}
