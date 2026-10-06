# Tests de la toolchain COS (OpenTofu + provider `orchestrator`)

Tests de bout en bout du produit COS à travers le provider `orchestrator` : chaque
scénario crée de vraies souscriptions sur l'orchestrateur de l'environnement (instance
COS, backup vault, buckets), les met à jour, vérifie le résultat et détruit tout.

Tout se lance avec un seul script, `toolchain_env.py`, qui fait à ta place ce qu'il
fallait faire à la main (login, init, proxy, token Vault, API key IBM) puis lance
`tofu test`.

```
toolchain_tests/
├── toolchain_env.py            prépare l'environnement et lance tofu
├── test_toolchain_env.py       tests unitaires du script (python -m pytest tests/toolchain_tests)
├── terraform/                  root unique de `tofu test`
│   ├── main.tf                 LES VRAIES RESSOURCES : cos, vault (count), buckets (for_each)
│   ├── variables.tf            contexte (environment, realm, apcode, tier, prefix) + ce que les
│   │                           scénarios font varier : scenario, with_vault, buckets (map typée)
│   ├── versions.tf             GÉNÉRÉ par toolchain_env.py : version du provider de l'environnement
│   ├── providers.tf            provider "orchestrator" {} (API key lue dans l'environnement)
│   ├── envs/<env>.tfvars       ce qui change par environnement : realm + version du provider
│   └── tests/*.tftest.hcl      un fichier par fonctionnalité (voir la stratégie)
└── deprecated/                 anciens test.tf monolithiques
```

---

## 1. Pourquoi OpenTofu

- Les tests reposent sur la commande `test` (fichiers `.tftest.hcl`, blocs `run`, assertions,
  destruction automatique). Elle n'existe en version stable qu'à partir de la **1.6** ; le poste
  était en Terraform 1.5.7, où elle est expérimentale avec une autre syntaxe.
- À partir de la 1.6, **Terraform est sous licence BUSL**, non libre, interdite ici.
- **OpenTofu** (`tofu`) est le fork communautaire sous licence MPL, maintenu par la Linux
  Foundation. Mêmes commandes, mêmes fichiers `.tf` et `.tftest.hcl`, même protocole de
  providers, même fichier `.terraform.lock.hcl`. Le script n'utilise que `tofu`.

Deux différences à connaître, gérées par le script :

| Point | Terraform | OpenTofu | Dans le script |
|---|---|---|---|
| source de provider sans hôte (`bp2i/orchestrator`) | `registry.terraform.io/...` | `registry.opentofu.org/...` | `versions.tf` est généré avec la source qualifiée `registry.terraform.io/bp2i/orchestrator`, pour rester sur le miroir Artifactory déjà configuré |
| fichier écrit par `login` | `~/.terraform.d/credentials.tfrc.json` | `~/.terraform.d/credentials.tofurc.json` | les deux sont lus ; un ancien `terraform login` reste valable |
| fichier de configuration CLI | `~/.terraformrc` | `~/.tofurc`, sinon `~/.terraformrc` | rien à changer si tu avais un `~/.terraformrc` (miroir de providers) |

## 2. Installation, une seule fois

### OpenTofu

```bash
brew install opentofu          # macOS
tofu version                   # attendu : OpenTofu v1.6 ou plus (1.7+ conseillé : vrai -filter)
```

Sans Homebrew, ou si `github.com` est bloqué par le proxy : binaire sur
<https://github.com/opentofu/opentofu/releases> (ou sur l'Artifactory interne), à mettre dans
le PATH. Pour faire cohabiter plusieurs versions : `tenv tofu install 1.9.0 && tenv tofu use 1.9.0`.

Si le poste avait un `terraform` : le laisser ou le retirer, le script ne l'appelle plus.

### Login sur le registre Artifactory (provider `orchestrator`)

```bash
tofu login repo.artifactory-dogen.group.echonet
```

Le token va dans `~/.terraform.d/credentials.tofurc.json`. Le script vérifie sa présence à
chaque lancement et ne relance `tofu login` que s'il manque (ou `TF_TOKEN_repo_artifactory__dogen_group_echonet`).

### Python

Python 3.10+, sans dépendance : le script n'utilise que la bibliothèque standard.

## 3. Ce que fait `toolchain_env.py`, étape par étape

Chaque étape est sautée quand elle est déjà faite et encore valable.

| # | Étape | Sautée si… |
|---|---|---|
| 1 | `tofu login repo.artifactory-dogen.group.echonet` | un credential est déjà enregistré |
| 2 | `versions.tf` réécrit avec `provider_version` de `envs/<env>.tfvars` | inchangé |
| 3 | `tofu init` dans `terraform/` (`-upgrade` si la version du provider a changé) | `.terraform/` existe déjà (`--reinit` pour forcer) |
| 4 | **proxy** `http://<user>:<mdp>@ncproxy.fr.net.intra:8080` : user et mot de passe demandés **une fois**, testés sur `iam.cloud.ibm.com` (407 = identifiants refusés), puis mémorisés | le mot de passe mémorisé est encore accepté ; ou `https_proxy` déjà exporté dans le shell **et** il répond (sinon il est abandonné pour ncproxy) ; ou `--no-proxy` |
| 5 | **token Vault** : `GET https://s02vl9956141:4430/v1/token/<uid>?namespace=AP85135` (service token, joint en direct, puis en IPv4 seul, puis via le proxy) ; `auth.client_token` est un token Vault de 30 jours | le token sauvegardé est encore accepté (`lookup-self`), ou `$VAULT_TOKEN` / `--vault-token` |
| 6 | **API key IBM Cloud** : `GET <vault>/v1/ibm_<compte>/creds/<rôle>_buhub` avec `X-Vault-Token` et `X-Vault-Namespace: AP85135` | l'API key sauvegardée a encore un lease valide |
| 7 | variables exportées (en mode `eval`, l'API key et le mot de passe du proxy ne sont jamais imprimés : le shell les lit lui-même dans le trousseau via `security` ; sans trousseau, `--run` / `--shell`) : `IBM_CLOUD_API_KEY`, `ORCHESTRATOR_IBMCLOUD_API_KEY`, `http_proxy` / `https_proxy` / `no_proxy` (minuscules et majuscules : tofu lit les majuscules d'abord, curl l'inverse), `TF_VAR_prefix` (ton user) | jamais |
| 8 | `tofu <commande> -var-file=envs/<env>.tfvars …` dans `terraform/`, journal dans `terraform/logs/` et lignes importantes en direct | seulement avec `--run` |

Ce qui est mémorisé, et où :

- **Secrets** (token Vault, API key, mot de passe du proxy) : dans le **trousseau macOS**
  (service `cos-toolchain`, visible dans Trousseaux d'accès). Sans trousseau (Linux, Windows),
  ils ne vivent que le temps du processus et sont redemandés au lancement suivant. Aucun secret
  n'est jamais écrit en clair sur le disque ; un `state.json` d'une ancienne version qui en
  contenait est purgé à la première lecture.
- **Réglages** non sensibles, dans `~/.cache/cos-toolchain/state.json` (0600) : lease de l'API
  key, uid pour le service token, user proxy, versions, et l'index des secrets mémorisés.

Le mot de passe du proxy est mémorisé après sa première vérification. S'il est refusé un jour
(mot de passe changé), il est oublié et redemandé. `--new-proxy-password` pour en saisir un
autre, `--forget-proxy-password` pour l'oublier, `--forget` efface tout.

**TLS** : tous les appels HTTPS du script (Vault, service token, test du proxy) vérifient le
certificat du serveur, sans option pour désactiver la vérification. Les CA internes sont lues
automatiquement dans les trousseaux système macOS ; ailleurs, passer un bundle PEM avec
`--ca-bundle`, `$COS_TOOLCHAIN_CA_BUNDLE` ou `$SSL_CERT_FILE`. Le Chrome / Edge piloté pour
le login SSO n'ignore plus les erreurs de certificat.

### Environnement → Vault → secret

| `--env` | Vault | service token | secret lu | tfvars |
|---|---|---|---|---|
| `int` | `hvault-dev.fr.net.intra` | `https://s02vl9956141:4430` | `ibm_ac002i000263/creds/rl002i000138_buhub` | `envs/int.tfvars` |
| `qual`, `pprod` | `hvault.staging.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000263/creds/rl002i000077_buhub` (à vérifier) | `envs/<env>.tfvars` |
| `prod` | `hvault.group.echonet` | à renseigner (UI Vault en attendant) | `ibm_ac002i000266/creds/rl002i000102_buhub` | `envs/prod.tfvars` |

Namespace Vault : `AP85135`. Tout est dans `ENVIRONMENTS`, `VAULTS` et `TOKEN_SERVICES` en tête
du script ; le realm et la version du provider dans `envs/<env>.tfvars`. `no_proxy` vaut
`localhost,127.0.0.1,.echonet,0.0.0.0` plus l'hôte du service token : les Vault `.echonet` sont
joints en direct, `hvault-dev.fr.net.intra` via le proxy.

### Secours quand le service token n'est pas disponible

`--browser-token` : Chrome / Edge est lancé avec un profil temporaire, en navigation privée,
avec le protocole DevTools ; le script ouvre l'UI Vault, attend le login SSO, lit le token de
session dans la page, ferme le navigateur. `--manual-token` : l'UI s'ouvre dans le navigateur
par défaut, « Copy token » dans le menu utilisateur, puis Entrée dans le terminal.

## 4. Lancer les tests

```bash
cd tests/toolchain_tests

python toolchain_env.py --env int --run test                                   # tous les scénarios, un tofu par fichier, l'un après l'autre
python toolchain_env.py --env int --run test --parallel                        # tous en même temps
python toolchain_env.py --env int --run test --parallel 3                      # trois à la fois
python toolchain_env.py --env int --run test -- -filter=tests/30_retention.tftest.hcl  # un seul
python toolchain_env.py --env int --run test -- -verbose                       # plans et state à chaque run
python toolchain_env.py --env int --run test --follow debug                    # tout le détail du provider en direct
python toolchain_env.py --env pprod --run test -- -filter=tests/20_bucket_lifecycle.tftest.hcl

python toolchain_env.py                     # mode guidé : menus numérotés
python toolchain_env.py --env int --shell   # sous-shell avec tout exporté, puis `tofu test` à la main
eval "$(python toolchain_env.py --env int)" # exporte dans le shell courant (secrets lus dans le trousseau par le shell)
```

Premier lancement après installation d'OpenTofu, ou après un changement de version du
provider : ajouter `--reinit`.

`--parallel` lance un processus `tofu test -filter=<fichier>` par scénario, car tofu lui-même
les enchaîne un par un. C'est possible parce que les scénarios sont indépendants : l'instance
COS est partagée et chaque scénario crée ses propres buckets. Chaque processus écrit dans son
journal `logs/<horodatage>-<env>-<scénario>.log` ; le script affiche le verdict de chacun au
fil de l'eau, puis un récapitulatif avec le chemin des journaux en échec. Demande tofu 1.7+.

Options utiles : `--uid`, `--proxy-user` / `--proxy-password` (ou `$PROXY_USER` /
`$PROXY_PASSWORD`) pour un lancement sans aucune saisie (CI), `--new-proxy-password`,
`--forget-proxy-password`, `--no-proxy`, `--prefix` (préfixe des descriptions),
`--follow`, `--tf-log`, `--no-log-file` (voir ci-dessous), `--new-token`, `--new-key`, `--forget`, `--probe` (diagnostic
réseau du service token), `--vault` / `--vault-url` / `--token-service` / `--secret-path` /
`--namespace` pour sortir des valeurs par défaut. `--help` liste tout.

### Voir ce qui se passe pendant un run

`tofu test` n'écrit une ligne qu'à la **fin** de chaque `run` (`run "create_bucket"... pass`) :
pendant les minutes où l'orchestrateur crée la souscription, le terminal reste muet, et
`-verbose` n'ajoute le plan et le state qu'après coup. Avec `--run`, le script active donc le
journal de tofu et du provider :

- le journal complet (`TF_LOG=debug` : appels du provider à l'orchestrateur, attente des
  souscriptions, erreurs) est écrit dans `terraform/logs/<horodatage>-<env>-<commande>.log` ;
  son chemin est affiché au début et à la fin ;
- les lignes `info`, `warn` et `error` sont recopiées en direct dans le terminal, préfixées
  par `|`, entre les lignes de `tofu test` :

```
tests/20_bucket_lifecycle.tftest.hcl... in progress
  | 14:02:11 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos
  | 14:03:40 [INFO]  Starting apply for orchestrator_subscription_cosbucket_v1.bucket["basic"]
  run "create"... pass
```

| Option | Effet |
|---|---|
| `--follow debug` (ou `trace`) | tout le détail en direct, dont les requêtes du provider |
| `--follow warn` / `error` | seulement les avertissements / les erreurs |
| `--follow off` | rien en direct ; `tail -f terraform/logs/<fichier>.log` dans un autre terminal |
| `--tf-log trace` | niveau du journal (défaut `debug`) |
| `--no-log-file` | pas de journal ; avec `--tf-log`, `TF_LOG` sort brut sur le terminal, comme avant |

Les journaux sont lisibles par toi seul (un journal `debug` peut contenir des en-têtes HTTP),
ignorés par git, et seuls les 20 derniers sont gardés. Ce que les lignes du provider
contiennent dépend de ce que le provider `orchestrator` journalise : si `info` est trop
pauvre ou trop bavard, changer `DEFAULT_FOLLOW_LEVEL` en tête du script.

## 5. Comment fonctionnent les tests (`tofu test`)

Le principe est celui du `test.tf` d'avant : un `apply`, on regarde, on modifie, on refait un
`apply`, puis un `destroy`. La différence : un fichier décrit cette séquence, tofu la déroule
seul et vérifie des conditions à chaque étape.

- `terraform/main.tf` est une configuration normale : l'instance COS de `cos_instance`
  (tfvars, réutilisée par tous les scénarios ; vide = chaque scénario crée et détruit la
  sienne), un bucket par clé de la map `buckets`, et un backup vault seulement si un bucket
  demande une sauvegarde ou si `with_vault = true`. Le payload d'un bucket est construit à
  partir de cette map en n'envoyant que les clés renseignées (celles de `BucketCreatePayload`
  / `BucketUpdatePayload` des DAGs `cos.bucket.v1.*`).
- Un fichier `tests/30_retention.tftest.hcl` est un scénario : des blocs `run` dans l'ordre.
  Chaque `run` donne la map `buckets` complète et ses assertions lisent **directement les
  attributs des ressources** du provider :

```hcl
variables { scenario = "retention" }

run "create" {
  variables { buckets = { days = { retention = { minimum_days = 1, default_days = 2, maximum_days = 3 } } } }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name != ""
    error_message = "Bucket en rétention non créé."
  }
}

run "update_days_bounds" {
  variables { buckets = { days = { retention = { minimum_days = 1, default_days = 5, maximum_days = 10 } } } }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["days"].name == run.create.bucket_names["days"]
    error_message = "L'update de rétention a recréé le bucket."
  }
}
```

Déroulé d'un `tofu test` :

1. tofu prend les fichiers de `tests/` un par un, par ordre alphabétique. Chaque fichier est
   indépendant, avec son propre state en mémoire, jamais écrit sur disque.
2. Dans un fichier, les `run` s'exécutent de haut en bas sur ce même state. Au premier `run`,
   `apply` crée le COS et le bucket. Au second, tofu calcule la différence comme un `plan`
   normal : seul le payload change, donc update en place. Une clé de `buckets` qui reste est
   mise à jour, une clé qui disparaît est détruite, une clé nouvelle est créée.
3. Après chaque `run`, chaque `assert` est évalué ; une condition fausse marque le run `fail`
   avec son `error_message`, et le fichier continue.
4. En fin de fichier, réussite ou échec, tofu détruit tout ce qu'il a créé, en ordre inverse
   des dépendances (bucket, vault, cos). C'est ce qui remplace le fichier vidé à la main.
5. Il n'y a **aucune règle métier dans `variables.tf`** : unités de rétention, bornes, classes de
   stockage sont validées par les DAGs (`BucketRetention`, `StorageClass`) et testées dans
   `tests/unit/`. Les dupliquer ici reviendrait à tester une copie. À la place, les
   scénarios envoient aussi des payloads invalides et vérifient que l'orchestrateur les
   **refuse avec le motif du DAG**.

### Les scénarios

| Fichier | Couvre | Runs |
|---|---|---|
| `20_bucket_lifecycle` | bucket standard | create → update versioning → update permissions → destroy |
| `21_storage_classes` | vault, cold, smart | create ×3 → destroy |
| `30_retention` | rétention jours, années et format historique `default` / `minimum` / `maximum` (ADR 0001) | create ×3 → update des bornes en jours → destroy |
| `40_object_lock` | object lock en jours et en années | create ×2 → update des durées (5 ans = plafond) → destroy |
| `50_backup` | backup vault, bucket sauvegardé | vault → bucket → update rétention backup → désactivation → destroy |
| `60_refused_retention_over_five_years` | **refus** : 1900 jours > 5 ans | un run, doit être refusé |
| `61_refused_object_lock_without_versioning` | **refus** : object lock sans versioning | idem |
| `62_refused_retention_with_versioning` | **refus** : rétention + versioning | idem |
| `63_refused_bucket_inputs` | **tous les refus de validation d'un bucket** en un seul run (rétention : unités, bornes, plafond, format historique, choix d'immutabilité ; object lock ; backup sans vault ; instance COS, classe de stockage et choix inconnus) | un run, une clé de `buckets` par cas, chaque cas doit être refusé avec son motif |
| `64_refused_bucket_update_inputs` | **tous les refus de validation d'un update** de bucket : depuis un bucket vierge, un bucket en object lock et un bucket en rétention (rétention sur bucket verrouillé, object lock ou versioning sur bucket en rétention, arrêt du versioning sous object lock, bornes de rétention contre l'existant, backup sans vault ou sans versioning) | create (valide) → update (interdit), une clé par cas |

Les assertions restent minimales : la souscription a un `name`, et un update garde le même
`name` (une mise à jour qui recrée la souscription est un échec). Le reste est vérifié par le
DAG lui-même. Les descriptions des souscriptions commencent par `TF_VAR_prefix` (ton user)
puis le nom du scénario : facile à retrouver dans l'orchestrateur, et
`cos-subscriptions/subscriptions_cleanup.py` reste là en cas de coupure réseau au milieu d'un test.

### Stratégie : trois niveaux

1. **Règles métier** : tests unitaires Python des schémas et services (`python -m pytest`),
   sur chaque MR. Rien à dupliquer côté HCL.
2. **Scénarios par fonctionnalité sur INT** : à la demande, puis en nocturne (voir CI).
3. **Fumée en pprod / prod** (`20_bucket_lifecycle`) après une montée de version du
   provider ou de l'orchestrateur : même code, seul `envs/<env>.tfvars` change.

L'instance COS n'a plus de scénario à elle : la créer et la détruire ne testait rien que les
autres fichiers ne fassent déjà quand `cos_instance` est vide dans le tfvars (chaque scénario
crée alors la sienne). Pour tester la création d'une instance : vider `cos_instance` et lancer
`20_bucket_lifecycle`.

Alternatives écartées : Terratest (Go, plus puissant pour vérifier côté API IBM, mais une
seconde stack), pytest-terraform (idem en Python, envisageable pour vérifier côté S3 depuis
`bucketService`). tflint, checkov et `tofu validate` restent complémentaires en statique.

### Les cas de refus (`6x_refused_*`)

Sur une demande refusée par le DAG, le provider **fait échouer l'`apply`** en citant le motif :

```
Error: Cannot create subscription "3caf2901-…"
Demand create status is "CANCELLED" but should be "SUCCESS", status reason "… default_days (1900 days)
cannot be superior to 5 years …"
```

`tofu test` ne sait pas attendre une erreur de provider. Chaque cas de refus est donc un
fichier à un seul run, et `tests/expected_failures.json` donne, par fichier, le run attendu en
échec et le motif (`regex`) à retrouver dans la sortie. `toolchain_env.py --run test` lance un
tofu par fichier et, pour ces fichiers, rend son propre verdict : run en `fail` **et** motif
présent = ✔ « refus attendu, motif conforme ». Un run qui passe est une régression (le DAG
accepte ce qu'il doit refuser). Pour ajouter un cas : un fichier `6x_refused_<cas>.tftest.hcl`
et une entrée dans le manifeste.

#### Tous les refus d'un bucket en un seul fichier (`63_refused_bucket_inputs`)

Les buckets d'un run sont indépendants : tofu les applique tous et rapporte **un diagnostic
par bucket refusé**, chacun citant l'adresse `bucket["<clé>"]` et le motif du DAG. Le fichier
`63_refused_bucket_inputs.tftest.hcl` met donc tous les cas invalides dans un seul run, sur
l'instance COS existante du tfvars, une clé de `buckets` par cas ; dans le manifeste, son
entrée porte `cases` (clé → `message` + `regex`) au lieu d'un seul `regex`. Le juge attribue
chaque diagnostic à sa clé et rend un verdict par cas :

```bash
python toolchain_env.py --env int --run test -- -filter=tests/63_refused_bucket_inputs.tftest.hcl
```

```
  ✘ 63_refused_bucket_inputs (2 min) : 2 cas sur 30 NON conformes (28 refus avec le motif attendu)
    ✔ retention_mixed_units : « Retention must be set either in days or in years, not a mix of both »
    ✔ retention_zero : « minimum_days must be superior to 0 »
    ✘ object_lock_zero : ACCEPTÉ (aucune erreur sur ce cas) alors que le DAG doit refuser : « … »
    ✘ unknown_storage_class : refusé, mais sans le motif attendu « … »
    …
```

Un cas **accepté** (pas de diagnostic sur sa clé) est une régression ; le bucket créé est
détruit en fin de fichier par tofu. Un cas refusé **sans le motif attendu** signale un message
qui a changé côté DAG (ou, pour `unknown_storage_class` / `unknown_immutability_choice`, un
refus du provider lui-même plutôt que du schéma du payload) : adapter `regex` dans le manifeste.
Les cas qui demandent un backup vault (« specifications are not fully set », backup sans
versioning, rétention + backup) ne sont pas dans ce fichier : il ne crée que des buckets.
Pour ajouter un cas : une clé dans le run et la même clé dans `cases`.

Les fichiers `60`, `61` et `62` restent utilisables seuls ; leurs trois cas sont repris dans `63`.

`64_refused_bucket_update_inputs` fait la même chose pour l'**update** : un run `create` met
chaque clé dans un état de départ valide (bucket vierge, en object lock, ou en rétention
1 / 2 / 3 jours), puis un run `update` envoie à chaque clé le payload interdit pour son état.
Dans le manifeste, `prelude` vaut `["create"]` : si un bucket de départ n'est pas créé, le
refus n'est pas testé et le verdict le dit. Les bornes de rétention d'un bucket déjà en
rétention sont comparées à l'existant, en jours (une borne en années est convertie : 1 an
= 365 ou 366 jours selon la date).

```bash
python toolchain_env.py --env int --run test -- -filter=tests/64_refused_bucket_update_inputs.tftest.hcl
```

### Environnement persistant et restauration (`terraform/persistent`, `terraform/restore`)

`tofu test` détruit tout à la fin de chaque fichier : impossible d'y garder un bucket, d'y
déposer des fichiers et de le restaurer plus tard. Les ressources qui doivent **survivre aux
tests** vivent donc dans un root Terraform classique, avec un vrai state local
(`terraform.tfstate`, ignoré par git) :

| Root | Contenu | Cycle de vie |
|---|---|---|
| `terraform/persistent` | un backup vault + un bucket `saved` (versioning, `backup_retention_days = 7`) sur le COS partagé `cos_instance` | créé une fois, **mis à jour** par les `apply` suivants, jamais détruit par un test |
| `terraform/restore` | une demande de restauration du bucket `saved` à un point dans le temps | lit le state de `persistent`, ne crée ni ne détruit rien d'autre |

```bash
# 1. Créer (ou mettre à jour) le vault et le bucket sauvegardé
python toolchain_env.py --env int --dir terraform/persistent --run apply
python toolchain_env.py --env int --dir terraform/persistent --run plan      # drift ? doit dire "No changes"

# 2. Déposer des objets dans le bucket (console IBM ou ibmcloud cos), attendre une sauvegarde

# 3. Restaurer à un point dans le temps : déclenche le DAG cos.bucket.v1.restore
python toolchain_env.py --env int --dir terraform/restore --run apply -- \
    -var restore_point_in_time=2026-10-06T10:30:00Z
#   options : -var bucket_key=saved  -var target_bucket=<autre bucket>  -var recovery_range_id=<id>

# 4. Seulement quand on n'en a plus besoin
python toolchain_env.py --env int --dir terraform/persistent --run destroy
```

Pour modifier le bucket persistant (ajouter une classe de stockage, changer la rétention de
sauvegarde…), éditer `var.buckets` dans `terraform/persistent/variables.tf` puis relancer
`--run apply` : le provider envoie un **update**, jamais un delete/create. Pour ajouter un
second bucket sauvegardé, ajouter une clé à `var.buckets` (le vault est partagé).

Le payload envoyé par `restore` est celui de `BucketRestoreBackupVaultPayload`
(`backup_vault_name`, `target_bucket`, `app_code`, `realm`, `restore_point_in_time`,
`recovery_range_id` optionnel). **À confirmer** dans la doc du provider : le type de ressource
qui déclenche une action sur une souscription existante (`terraform/restore/main.tf` utilise
le nom provisoire `orchestrator_action_cosbucket_restore_v1`) ; tant qu'il n'est pas le bon,
`tofu validate` échoue sur ce root et rien n'est envoyé.

### Pièges connus

- La **version du provider** ne peut pas être une variable : `versions.tf` est généré depuis
  le tfvars. Ne pas l'éditer à la main.
- Les assertions lisent le `payload` que **le provider renvoie** ; s'il le normalise, adapter
  les assertions plutôt que `main.tf`.
- Le « second plan sans changement » (drift, ADR 0001) ne s'exprime pas en `.tftest.hcl` :
  `python toolchain_env.py --env int --run plan -- -detailed-exitcode` sur un `main.tf` ad hoc
  renvoie 2 s'il y a un drift.
- `-filter` n'existe qu'à partir de la 1.7 : en 1.6, le script l'émule en liant les fichiers
  demandés dans `.tftest-filter/` et en passant `-test-directory`.

### Vers la CI

Un job GitLab nocturne sur INT, avec le token Vault fourni au job (approle CI) :

```yaml
toolchain-tests:
  stage: toolchain
  rules: [{ if: '$CI_PIPELINE_SOURCE == "schedule"' }]
  script:
    - python tests/toolchain_tests/toolchain_env.py --env int --skip-login --no-proxy
        --prefix ci-$CI_PIPELINE_ID --vault-token "$VAULT_TOKEN"
        --run test --parallel -- -junit-xml=report.xml   # -junit-xml : OpenTofu 1.9+
  artifacts: { reports: { junit: report.xml } }
```

Sur chaque MR, seulement `tofu fmt -check -recursive` et `tofu validate` (pas d'infra).

## 6. Diagnostic

### Ce que le log doit montrer quand tout va bien

```
Environnement : int  dossier : .../toolchain_tests/terraform
Vault : https://hvault-dev.fr.net.intra (namespace AP85135)  secret : ibm_ac002i000263/creds/rl002i000138_buhub
tofu login : credentials déjà enregistrés pour repo.artifactory-dogen.group.echonet.
versions.tf : provider orchestrator 2.3.0-int (inchangé).
tofu init : déjà fait dans ... (--reinit pour refaire).
Proxy ncproxy.fr.net.intra:8080 : identifiants de ton compte (le mot de passe n'est pas sauvegardé).
Proxy OK (https://iam.cloud.ibm.com/... joignable).
Token Vault sauvegardé valide encore 29 jours.        (ou : Service token joint direct.)
API key sauvegardée réutilisée (...).                 (ou : API key lue dans Vault (..., lease 2 h).)
Variables exportées : IBM_CLOUD_API_KEY, ORCHESTRATOR_IBMCLOUD_API_KEY, http_proxy, ..., TF_VAR_prefix
Journal tofu (TF_LOG=debug) : .../terraform/logs/20261002-140200-int-test.log
$ tofu test -var-file=envs/int.tfvars ...
  | 14:02:11 [INFO]  Starting apply for ...           (lignes du journal, en direct)
Journal complet : .../terraform/logs/20261002-140200-int-test.log
```

### Problèmes rencontrés et leur cause

| Message | Cause | Quoi faire |
|---|---|---|
| `Cannot init client API … iam.cloud.ibm.com … authenticationrequired` | tofu sort vers IBM via un proxy sans identifiants valides (407) | laisser le script exporter le proxy ; à la main, vérifier `env \| grep -i proxy` (majuscules et minuscules) |
| `Proxy du shell inutilisable (… 503 Service Unavailable)` | un `https_proxy` du profil shell pointe sur un proxy local arrêté (ex. `127.0.0.1:8079`) | rien : le script bascule sur ncproxy ; ou `unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY` |
| `proxy … : identifiants refusés (HTTP 407)` | user ou mot de passe ncproxy faux, ou compte bloqué | le mot de passe mémorisé est oublié et redemandé ; `--new-proxy-password` pour forcer |
| `fichier de scénario introuvable … contient : …` | le `-filter` ne correspond à aucun fichier de `terraform/tests/` ; un nom listé avec une espace avant la virgule a un caractère parasite | le script prend le fichier quand même et le signale : renommer le fichier proprement |
| `service token injoignable` + sondage TCP | l'hôte `s02vl9956141` n'est pas joignable depuis le poste (VPN, DNS) | `--probe` pour le détail ; nom complet via `--token-service` ; `--browser-token` en attendant |
| `flag provided but not defined: -filter` | tofu 1.6 | le script émule ; ou passer en 1.7+ |
| `tofu 1.5.x : tofu test demande OpenTofu 1.6 au minimum` | binaire trop ancien | installer OpenTofu ≥ 1.6 puis `--reinit` |
| `tofu introuvable dans le PATH` | OpenTofu non installé | section 2 |
| le log `TF_LOG=debug` montre `"retention": {"retention_enabled": true}` alors que le run donne `minimum_days`, `default_days`… | le **provider** filtre `payload.retention` sur le schéma qu'il connaît (celui du contrat `BucketRetention` publié par le service) ; une version du provider ou du service qui ne connaît pas les champs `*_days` / `*_years` les supprime sans erreur, et le DAG ne reçoit que le flag | `tofu providers schema -json \| jq '.provider_schemas[].resource_schemas.orchestrator_subscription_cosbucket_v1'` et chercher `retention` : si les attributs sont figés sans `*_days`, il faut un provider (ou un déploiement du service INT) qui expose le nouveau contrat ; ce n'est pas un problème de `main.tf` |

Commandes de contrôle manuelles :

```bash
python toolchain_env.py --env int --probe          # sondage TCP + GET du service token par chaque chemin
curl -k -m 10 --noproxy '*' "https://s02vl9956141:4430/v1/token/lh90871?namespace=AP85135"
curl -sS -o /dev/null -w '%{http_code}\n' https://iam.cloud.ibm.com/identity/.well-known/openid-configuration   # 200 = proxy OK
```

### Lister les versions du provider disponibles sur Artifactory

```bash
HOST=repo.artifactory-dogen.group.echonet
TOKEN=$(jq -r ".credentials[\"$HOST\"].token" ~/.terraform.d/credentials.tofurc.json)
curl -sk -H "Authorization: Bearer $TOKEN" "https://$HOST/.well-known/terraform.json"      # chemin providers.v1
curl -sk -H "Authorization: Bearer $TOKEN" "https://$HOST<providers.v1>bp2i/orchestrator/versions" | jq -r '.versions[].version'
```

## 7. Tests du script

```bash
python -m pytest tests/toolchain_tests        # depuis cos/
```
