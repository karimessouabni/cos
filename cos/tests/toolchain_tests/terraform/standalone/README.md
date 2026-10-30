# Roots autonomes : COS + backup vault + bucket sauvegardé

Un dossier par environnement, sans aucune variable : `environment`, `realm`,
`apcode`, `tier` et la version du provider sont écrits en dur dans `main.tf` et
`versions.tf` (valeurs reprises de `../envs/<env>.tfvars`). Chaque root crée,
dans l'ordre imposé par les références :

1. une instance COS (`orchestrator_subscription_cos_v1`) ;
2. un backup vault sur cette instance (`orchestrator_subscription_cosbackup_vault_v1`) ;
3. un bucket versionné sauvegardé dans ce vault (`orchestrator_subscription_cosbucket_v1`,
   `backup_retention_days = 7`).

| dossier | environment orchestrator | realm | provider |
|---------|--------------------------|-------|----------|
| `int/`   | `int`  | `rl002i000138` | `2.4.0-rc.2-int` |
| `qual/`  | `qual` | `rl002i000138` (à vérifier) | `2.4.0-rc.2-qual` (à vérifier) |
| `pprod/` | `qua`  | `rl002i000077` | `2.4.0-rc.2-pprod` |
| `prod/`  | `prod` | `rl002i000102` (à vérifier) | `2.3.0` (à vérifier) |

## Lancer

Depuis `cos/tests/toolchain_tests` (clé API, proxy, `tofu login`, `tofu init` et
journal `TF_LOG` gérés par le toolchain) :

```
python toolchain_env.py --env pprod --dir terraform/standalone/pprod --run apply
python toolchain_env.py --env pprod --dir terraform/standalone/pprod --run plan
python toolchain_env.py --env pprod --dir terraform/standalone/pprod --run destroy
```

Ou à la main, dans le dossier de l'environnement, avec
`ORCHESTRATOR_IBMCLOUD_API_KEY` (et le proxy) exportés :

```
tofu init
tofu apply
tofu destroy
```

Le state est local (`terraform.tfstate`, ignoré par git) : plan et destroy se
font depuis le même dossier que l'apply. Comme il n'y a pas de
`envs/<env>.tfvars` dans ces dossiers, `toolchain_env.py` n'ajoute pas de
`-var-file` et ne réécrit pas `versions.tf`.

## Différences avec `../main.tf`

- L'instance COS est toujours créée (et détruite au destroy), alors que les
  scénarios réutilisent `cos_instance` du tfvars. Pour réutiliser une instance
  existante : supprimer la ressource `cos` et remplacer `local.cos_name` par
  son nom.
- Les descriptions commencent par `standalone <env>` au lieu de
  `<prefix> <scenario>` : `TF_VAR_prefix` n'a pas d'effet ici.
