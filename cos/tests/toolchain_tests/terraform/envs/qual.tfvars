environment      = "qual"
realm            = "rl002i000138"    # à vérifier
cos_instance     = ""                # à renseigner : instance COS existante à réutiliser (vide = créée par chaque scénario)
provider_version = "2.4.0-rc.2-qual" # à vérifier : même rc que pprod, suffixe de l'environnement
apcode           = "AP85135"
tier             = "P"
prefix           = "toolchain" # surchargé par TF_VAR_prefix (toolchain_env.py : user)
