# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Sauvegarde : refus : refus « empty_vault_name »
#
# Sauvegarde : refus : cas de refus « empty_vault_name ». Le provider fait
# échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie porte
# le motif du DAG (tests/expected_failures.json).

variables {
  scenario   = "backup rules empty_vault_name"
  with_vault = true
}

# Attendu :
#   r : create -> REFUSÉ (dag) « The Backup Vault name is required to enable bucket backup »
run "empty_vault_name" {
  variables {
    buckets = {
      r = { enable_versioning = true, backup_retention_days = 7, backup_vault_name = "" }
    }
  }
}
