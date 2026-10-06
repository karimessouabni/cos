# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « unknown_vault »
#
# Sauvegarde : refus : cas de refus « unknown_vault ». Le provider fait échouer
# l'apply du run refusé ; toolchain_env.py vérifie que la sortie porte le motif
# du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules unknown_vault"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (dag) « The Backup Vault doesn't exist for the name : vault-inconnu-toolchain »
run "unknown_vault" {
  variables {
    buckets = {
      r = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "vault-inconnu-toolchain" }
    }
  }
}
