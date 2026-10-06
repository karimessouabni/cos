# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Classes de stockage : refus « unknown_class »
#
# Classes de stockage : cas de refus « unknown_class ». Le provider fait échouer
# l'apply du run refusé ; toolchain_env.py vérifie que la sortie porte le motif
# du DAG (tests/expected_failures.json).

variables {
  scenario = "storage classes unknown_class"
}

# Attendu :
#   glacier : create -> REFUSÉ (schema) « Input should be 'standard', 'vault', 'cold' or 'smart' »
run "unknown_class" {
  variables {
    buckets = {
      glacier = { storage_class = "glacier" }
    }
  }
}
