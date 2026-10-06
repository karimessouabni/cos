# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Contexte de la demande : refus « unknown_cos_instance »
#
# Contexte de la demande : cas de refus « unknown_cos_instance ». Le provider
# fait échouer l'apply du run refusé ; toolchain_env.py vérifie que la sortie
# porte le motif du DAG (tests/expected_failures.json).

variables {
  scenario = "context rules unknown_cos_instance"
}

# Attendu :
#   r : create -> REFUSÉ (dag) « the cos instance co000000000000 doesn't exist »
run "unknown_cos_instance" {
  variables {
    buckets = {
      r = { cos_instance = "co000000000000" }
    }
  }
}
