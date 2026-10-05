environment      = "prod"
realm            = "rl002i000102" # à vérifier
cos_instance     = ""             # à renseigner : instance COS existante à réutiliser (vide = créée par chaque scénario)
provider_version = "2.3.0"        # à vérifier
apcode           = "AP85135"
tier             = "P"
prefix           = "toolchain" # surchargé par TF_VAR_prefix (toolchain_env.py : user)
