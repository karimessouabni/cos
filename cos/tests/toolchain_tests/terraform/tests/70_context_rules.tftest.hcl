# GÉNÉRÉ par generate_tests.py depuis scenario_matrix.py : ne pas éditer à la main.
# Contexte de la demande
#
# Instance COS inconnue : refus par validate_request du DAG.

variables {
  scenario = "context rules"
}

# Attendu :
#   r : create -> REFUSÉ (dag) « the cos instance co000000000000 doesn't exist »
run "unknown_cos_instance" {
  variables {
    buckets = {
      r = { cos_instance = "co000000000000" }
    }
  }

  assert {
    condition     = output.bucket_status["r"] == "DECLINED"
    error_message = "r (create) : aurait dû être refusé (the cos instance co000000000000 doesn't exist) ; status = ${jsonencode(output.bucket_status["r"])}."
  }
  assert {
    condition     = can(regex("the cos instance co000000000000 doesn't exist", output.bucket_status_reason["r"]))
    error_message = "r (create) : motif inattendu : ${output.bucket_status_reason["r"]}"
  }
}
