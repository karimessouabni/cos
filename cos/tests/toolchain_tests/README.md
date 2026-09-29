# Tests Terraform de la toolchain

Tests de bout en bout du produit COS à travers le provider `orchestrator`
(`source = "bp2i/orchestrator"`, téléchargé depuis `repo.artifactory-dogen.group.echonet`) :
chaque scénario crée de vraies souscriptions sur l'orchestrateur de l'environnement
(instance COS, backup vault, buckets), les met à jour, vérifie le résultat et détruit tout.

```
toolchain_tests/
├── toolchain_env.py            prépare l'environnement et lance terraform (voir plus bas)
├── terraform/                  root unique de `terraform test`
│   ├── versions.tf             GÉNÉRÉ par toolchain_env.py : version du provider de l'environnement
│   ├── providers.tf            provider "orchestrator" {} (API key lue dans l'environnement)
│   ├── variables.tf            environment, realm, provider_version, apcode, tier, prefix
│   ├── envs/<env>.tfvars       ce qui change par environnement : realm + version du provider
│   ├── modules/
│   │   ├── cos/                orchestrator_subscription_cos_v1
│   │   ├── backup_vault/       orchestrator_subscription_cosbackup_vault_v1
│   │   ├── bucket/             orchestrator_subscription_cosbucket_v1 + payload typé + validations
│   │   └── buckets/            N buckets en un run (for_each) sur la même instance COS
│   └── tests/*.tftest.hcl      un fichier par fonctionnalité (voir la stratégie)
└── int/, pprod/, ...           anciens test.tf monolithiques (à supprimer quand la suite est adoptée)
```

## Stratégie de test

### Ce qui existe sur le marché, et le choix

| Outil | Principe | Pourquoi / pourquoi pas ici |
|---|---|---|
| **`terraform test`** (natif, TF ≥ 1.6) | fichiers `.tftest.hcl`, `run` séquentiels qui appliquent des modules, assertions HCL, destruction automatique en fin de fichier | **retenu** : aucune dépendance, même langage que les tests, cycle create → update → destroy garanti, `expect_failures` pour les garde-fous |
| Terratest (Go) | tests Go qui lancent terraform et interrogent l'API | plus puissant (appels API IBM pour vérifier le bucket réel) mais une seconde stack à maintenir |
| pytest-terraform / tftest (Python) | idem en Python | envisageable plus tard pour vérifier côté S3 (versioning, object lock) depuis `bucketService` |
| tflint, checkov, `terraform validate` | statique, sans infra | à mettre en CI sur chaque MR, complémentaire |

### Les trois niveaux

1. **Garde-fous, sans infra** — `tests/00_validation.tftest.hcl`, `command = plan` : les règles de
   l'ADR 0001 (jours OU années, `minimum <= default <= maximum`, plafond 5 ans) et les classes
   de stockage sont vérifiées par les `validation` du module bucket, avec `expect_failures`.
   Quelques secondes, à lancer sur chaque MR.
2. **Scénarios par fonctionnalité, sur INT** — un fichier par feature, chacun autonome
   (il crée son instance COS) et donc filtrable et parallélisable :

   | Fichier | Couvre | Runs |
   |---|---|---|
   | `10_cos.tftest.hcl` | instance COS | create → destroy |
   | `20_bucket_basic.tftest.hcl` | bucket standard | create → update versioning → update custom permissions → destroy |
   | `21_bucket_storage_classes.tftest.hcl` | vault, cold, smart | create ×3 (un run, module `buckets`) → destroy |
   | `30_bucket_retention.tftest.hcl` | rétention jours et années (ADR 0001) | create ×2 → update des bornes en jours → destroy |
   | `40_bucket_immutability.tftest.hcl` | object lock | create (durée 1 j + versioning) → update durée → destroy |
   | `50_bucket_backup.tftest.hcl` | backup vault | cos → vault → bucket sauvegardé → update rétention backup → destroy (ordre inverse) |

   Chaque `update` vérifie que `output.name` n'a pas changé : une mise à jour qui recrée la
   souscription est un échec. La destruction est faite par `terraform test` lui-même, en ordre
   inverse des `run`, même quand une assertion échoue : plus de fichier `.tf` vidé à la main ni
   de souscriptions oubliées (en cas de coupure réseau, `subscriptions_cleanup.py` reste là).
3. **Fumée en pprod / prod** — `-filter=tests/10_cos.tftest.hcl` (ou `20_`) après une mise à jour
   du provider ou de l'orchestrateur : même code, seul `envs/<env>.tfvars` change.

### Pièges connus de `terraform test`

- Deux `run` sur le **même module partagent un seul state** : un second `run` sur
  `modules/bucket` est une mise à jour, pas une création. Pour plusieurs buckets indépendants
  dans un fichier, passer par `modules/buckets` (map + `for_each`).
- La **version du provider** ne peut pas être une variable : `versions.tf` est réécrit par
  `toolchain_env.py` depuis `provider_version` du tfvars (suivi d'un `init -upgrade` quand elle
  change). Ne pas l'éditer à la main.
- Les assertions portent sur ce que le provider renvoie dans `payload` ; si le provider
  normalise le payload, adapter les assertions plutôt que le module.
- Le « second plan sans changement » (drift, ADR 0001) ne s'exprime pas en `.tftest.hcl` :
  `python toolchain_env.py --env int --run plan -- -detailed-exitcode` sur un `main.tf` ad hoc
  renvoie 2 s'il y a un drift.

### Lancer

```bash
cd tests/toolchain_tests
python toolchain_env.py --env int --run test                                   # tout
python toolchain_env.py --env int --run test -- -filter=tests/30_bucket_retention.tftest.hcl
python toolchain_env.py --env int --run test -- -verbose                       # plans et outputs
python toolchain_env.py --env pprod --run test -- -filter=tests/10_cos.tftest.hcl
```

`toolchain_env.py` ajoute `-var-file=envs/<env>.tfvars`, régénère `versions.tf`, et exporte
`TF_VAR_prefix` (ton user) : les descriptions des souscriptions créées commencent par ton
user, ce qui permet de retrouver et nettoyer tes tests dans l'orchestrateur.

### Vers la CI

Un job GitLab nocturne sur INT, avec l'API key lue dans Vault par le job (approle CI) :

```yaml
toolchain-tests:
  stage: toolchain
  rules: [{ if: '$CI_PIPELINE_SOURCE == "schedule"' }]
  script:
    - export TF_VAR_prefix=ci-$CI_PIPELINE_ID
    - python tests/toolchain_tests/toolchain_env.py --env int --skip-login --no-proxy
        --vault-token "$VAULT_TOKEN" --run test -- -junit-xml=report.xml   # -junit-xml : TF ≥ 1.11
  artifacts: { reports: { junit: report.xml } }
```

Sur chaque MR, seulement le niveau 1 (`-filter=tests/00_validation.tftest.hcl`), plus
`terraform fmt -check -recursive` et `terraform validate`.

## Préparer l'environnement : `toolchain_env.py`

Avant, il fallait à la main : `terraform login`, `terraform init`, exporter le proxy avec
son user et son mot de passe, appeler le service token (`GET /v1/token/<uid>`) dans son
Swagger, se connecter à l'UI Vault avec le `client_token` obtenu, le recopier depuis son
profil, le coller dans un `curl` pour lire l'API key IBM Cloud, puis exporter
`IBM_CLOUD_API_KEY` et `ORCHESTRATOR_IBMCLOUD_API_KEY`. Le script enchaîne tout cela et
ne refait que ce qui est nécessaire :

| Étape | Sautée si… |
|---|---|
| `terraform login <artifactory>` | un credential est déjà dans `~/.terraform.d/credentials.tfrc.json` (ou `TF_TOKEN_…`) |
| `terraform init` | `.terraform/` existe déjà dans le dossier (`--reinit` pour forcer) |
| proxy `http://<user>:<mdp>@ncproxy.fr.net.intra:8080` (user et mot de passe demandés, puis testés sur `iam.cloud.ibm.com` : 407 = refusés) | `https_proxy` est déjà exporté dans le shell et répond (sinon ncproxy est demandé), ou `--no-proxy` |
| token Vault : `GET <service token>/v1/token/<uid>?namespace=AP85135` → `auth.client_token` | le token sauvegardé est encore accepté (`lookup-self`), ou `$VAULT_TOKEN` / `--vault-token` |
| `GET <vault>/v1/ibm_<compte>/creds/<rôle>_buhub` | l'API key sauvegardée a encore un lease valide |
| export des variables | jamais |

Le `client_token` renvoyé par le service token est directement un token Vault (30 jours) :
c'est celui que l'UI affiche ensuite dans « Copy token ». Le script l'utilise donc tel
quel, sans passer par l'UI. L'uid (ex. `lh90871`) est demandé au premier lancement puis
mémorisé, comme le user du proxy ; le mot de passe du proxy est redemandé à chaque
lancement et n'est jamais écrit sur disque. Le cache est dans
`~/.cache/cos-toolchain/state.json` (lisible par soi seul).

```bash
cd tests/toolchain_tests

# tout enchaîner puis terraform test / plan / apply dans terraform/
python toolchain_env.py --env int --run test
python toolchain_env.py --env int --run plan

# exporter les variables (API key + proxy) dans le shell courant, puis terraform normalement
eval "$(python toolchain_env.py --env int)"
terraform -chdir=int/new_version plan

# un sous-shell avec les variables déjà exportées
python toolchain_env.py --env int --shell

# menus numérotés (défaut sans argument dans un terminal)
python toolchain_env.py
```

Options utiles : `--uid`, `--proxy-user` / `--proxy-password` (ou `$PROXY_USER` /
`$PROXY_PASSWORD`) pour ne rien saisir, `--no-proxy`, `--dir` (autre dossier terraform),
`--tf-log` (`TF_LOG=debug`), `--new-token`, `--new-key`, `--forget`, `--browser-token`
(Chrome sur l'UI Vault au lieu du service token), `--manual-token` (copier-coller depuis
l'UI Vault), `--vault` / `--vault-url` / `--token-service` / `--secret-path` /
`--namespace` pour sortir des valeurs par défaut. `--help` liste tout.

### Environnement → Vault → secret

| `--env` | Vault | service token | secret lu |
|---|---|---|---|
| `int` | `hvault-dev.fr.net.intra` | `https://s02vl9956141:4430` | `ibm_ac002i000263/creds/rl002i000138_buhub` |
| `qual`, `pprod` | `hvault.staging.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000263/creds/rl002i000077_buhub` (à vérifier) |
| `prod` | `hvault.group.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000266/creds/rl002i000102_buhub` |

Namespace Vault : `AP85135`. Tout est dans `ENVIRONMENTS`, `VAULTS` et `TOKEN_SERVICES`
en tête du script. `no_proxy` vaut `localhost,127.0.0.1,.echonet,0.0.0.0` : les Vault
`.echonet` sont joints en direct, `hvault-dev.fr.net.intra` via le proxy, et l'hôte du service token (`s02vl9956141`) est ajouté automatiquement à `no_proxy` : il n'est joignable qu'en direct.

### Secours : token lu dans Chrome

Sans service token pour l'instance Vault (ou avec `--browser-token`), même mécanisme que
`cos-subscriptions/subscriptions_cleanup.py` : Chrome / Edge est lancé avec un profil
temporaire, en navigation privée, avec le protocole DevTools activé sur un port local. Le
script ouvre l'UI Vault, attend le login, puis évalue dans la page une expression JS qui
cherche le token de session dans `sessionStorage` / `localStorage`. Dès qu'un token est
trouvé, le navigateur est fermé et le profil supprimé. Sans Chrome, ou avec
`--manual-token`, l'UI s'ouvre dans le navigateur par défaut : « Copy token » dans le
menu utilisateur, puis Entrée dans le terminal.

## Diagnostic : le service token ne répond pas

Le script essaie de le joindre en direct, en direct IPv4 seulement, puis via le proxy,
et affiche un sondage TCP par adresse quand tout échoue. Pour ne faire que ce test :

```bash
python toolchain_env.py --env int --probe
```

À la main, les trois commandes équivalentes (direct, direct depuis Python, via le proxy) :

```bash
curl -k -m 10 --noproxy '*' "https://s02vl9956141:4430/v1/token/lh90871?namespace=AP85135"

python3 -c "import urllib.request as u,ssl; o=u.build_opener(u.ProxyHandler({}),u.HTTPSHandler(context=ssl._create_unverified_context())); print(o.open('https://s02vl9956141:4430/v1/token/lh90871?namespace=AP85135',timeout=15).read()[:120])"

curl -k -m 10 -x "$https_proxy" "https://s02vl9956141:4430/v1/token/lh90871?namespace=AP85135"
```

| Résultat | Cause | Quoi faire |
|---|---|---|
| curl direct OK, Python pend, sondage `IPv6 … timed out` | Python attend sur l'adresse IPv6 | rien : la route « IPv4 seulement » du script passe |
| `résolution DNS impossible` | nom court inconnu du poste | passer le nom complet : `--token-service https://s02vl9956141.<domaine>:4430` |
| tout pend, curl aussi | VPN / réseau | `--browser-token` en attendant |
| `HTTP 4xx` | hôte joint, uid ou namespace refusé | `--uid`, `--namespace` |

## Tests du script

```bash
python -m pytest tests/toolchain_tests        # depuis cos/
```
