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
| 7 | variables exportées (en mode `eval`, l'API key et le mot de passe du proxy ne sont jamais imprimés : le shell les lit lui-même dans le trousseau via `security` ; sans trousseau, `--run` / `--shell`, ou `--print-secrets`) : `IBM_CLOUD_API_KEY`, `ORCHESTRATOR_IBMCLOUD_API_KEY`, `http_proxy` / `https_proxy` / `no_proxy` (minuscules et majuscules : tofu lit les majuscules d'abord, curl l'inverse), `TF_VAR_prefix` (ton user) | jamais |
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

python toolchain_env.py --env int --run test                                   # tous les scénarios, un par un
python toolchain_env.py --env int --run test --parallel                        # tous en même temps (un tofu par fichier)
python toolchain_env.py --env int --run test --parallel 3                      # trois à la fois
python toolchain_env.py --env int --run test -- -filter=tests/10_cos.tftest.hcl  # un seul
python toolchain_env.py --env int --run test -- -verbose                       # plans et state à chaque run
python toolchain_env.py --env int --run test --follow debug                    # tout le détail du provider en direct
python toolchain_env.py --env pprod --run test -- -filter=tests/10_cos.tftest.hcl

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
tests/20_bucket_basic.tftest.hcl... in progress
  | 14:02:11 [INFO]  Starting apply for orchestrator_subscription_cos_v1.cos
  | 14:03:40 [INFO]  Starting apply for orchestrator_subscription_cosbucket_v1.bucket["basic"]
  run "create_bucket"... pass
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

- `terraform/main.tf` est une configuration normale : une instance COS (celle de
  `cos_instance` dans le tfvars, réutilisée par tous les scénarios ; créée et détruite
  seulement par `10_cos`), un bucket par clé de
  la map `buckets`, et un backup vault seulement si un bucket demande une sauvegarde
  (`backup_retention_days`) ou si `with_vault = true`. Les scénarios rétention et object lock
  n'ont donc pas de vault. Le payload d'un bucket est construit
  à partir de cette map en n'envoyant que les clés renseignées (celles de `BucketCreatePayload`
  / `BucketUpdatePayload` des DAGs `cos.bucket.v1.*`).
- Un fichier `tests/20_bucket_basic.tftest.hcl` est un scénario : des blocs `run` dans l'ordre.
  Chaque `run` ne définit **que ce qui change** et ses assertions lisent **directement les
  attributs des ressources** du provider :

```hcl
variables { scenario = "bucket basic" }

run "create_bucket" {
  variables { buckets = { basic = {} } }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].payload.storage_class == "standard"
    error_message = "storage_class attendue : standard."
  }
}

run "update_enable_versioning" {
  variables { buckets = { basic = { enable_versioning = true } } }
  assert {
    condition     = orchestrator_subscription_cosbucket_v1.bucket["basic"].name == run.create_bucket.bucket_names["basic"]
    error_message = "L'update a recréé le bucket au lieu de le modifier en place."
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
   `tests/schemas/`. Les dupliquer ici reviendrait à tester une copie. Un payload refusé par
   l'orchestrateur fait simplement échouer le run.

### Les scénarios

| Fichier | Couvre | Runs |
|---|---|---|
| `10_cos.tftest.hcl` | instance COS | create → destroy (seul scénario qui en crée une ; les autres réutilisent `cos_instance` du tfvars) |
| `20_bucket_basic.tftest.hcl` | bucket standard | create → update versioning → update custom permissions → destroy |
| `21_bucket_storage_classes.tftest.hcl` | vault, cold, smart | create ×3 en un run → destroy |
| `30_bucket_retention.tftest.hcl` | rétention jours, années et format historique `default` / `minimum` / `maximum` (ADR 0001) | create ×3 → update des bornes en jours → destroy |
| `31_bucket_retention_limits.tftest.hcl` | bornes acceptées : 5 ans pile, 1826 jours, format historique au plafond, bornes égales | create ×4 → destroy |
| `40_bucket_immutability.tftest.hcl` | object lock en jours | create (durée 1 j + versioning) → update durée → destroy |
| `41_object_lock_years.tftest.hcl` | object lock en années | create 1 an → update 5 ans (plafond) → destroy |
| `50_bucket_backup.tftest.hcl` | backup vault (rattaché par `backup_vault_name`, le DAG résout le sub_id) | vault → bucket sauvegardé → update rétention backup → destroy |
| `60_immutability_failures.tftest.hcl` | **cas d'échec** : > 5 ans en jours, en années et au format historique, jours et années mélangés, historique et nouveau format mélangés, bornes incohérentes ou incomplètes, choix `_daily` / `_yearly` contredit par les valeurs, object lock > 5 ans, sans durée dans l'unité choisie, à 0, rétention et object lock ensemble | 19 runs, un payload invalide chacun, qui doivent tous être **refusés** |

Chaque `update` vérifie que `name` n'a pas changé : une mise à jour qui recrée la souscription
est un échec. Les descriptions des souscriptions commencent par `TF_VAR_prefix` (ton user) puis
le nom du scénario : facile à retrouver et à nettoyer dans l'orchestrateur, et
`cos-subscriptions/subscriptions_cleanup.py` reste là en cas de coupure réseau au milieu d'un test.

### Stratégie : trois niveaux

1. **Règles métier** : tests unitaires Python des schémas et services (`python -m pytest`),
   sur chaque MR. Rien à dupliquer côté HCL.
2. **Scénarios par fonctionnalité sur INT** : à la demande, puis en nocturne (voir CI).
3. **Fumée en pprod / prod** (`10_cos` ou `20_bucket_basic`) après une montée de version du
   provider ou de l'orchestrateur : même code, seul `envs/<env>.tfvars` change.

Alternatives écartées : Terratest (Go, plus puissant pour vérifier côté API IBM, mais une
seconde stack), pytest-terraform (idem en Python, envisageable pour vérifier côté S3 depuis
`bucketService`). tflint, checkov et `tofu validate` restent complémentaires en statique.

### Les cas d'échec et ce que le provider doit exposer

Les règles métier ne sont pas revérifiées côté Terraform (voir plus haut) : un cas d'échec
envoie le payload invalide à l'orchestrateur et vérifie que **le DAG l'a refusé**. Pour ça,
`main.tf` expose `bucket_status` et `bucket_status_reason`, lus sur la ressource avec
`try(b.status, b.state.status, null)` : les noms de la console orchestrator (`status`,
`status_reason` de la demande). Deux cas selon le provider :

- il remonte la demande refusée dans la ressource : les assertions de `60_*` comparent
  `status == "DECLINED"` et le motif au message du DAG ;
- il fait échouer l'`apply` sur un refus : les runs de `60_*` sont alors en `fail` pour la
  bonne raison, mais tofu ne sait pas l'attendre (`expect_failures` ne couvre pas les erreurs
  de provider). Dans ce cas, dis-le : on gardera `60_*` comme suite « doit échouer » lancée
  à part, et les motifs restent couverts par les tests unitaires Python.

Au premier lancement de `60_*` sur INT, regarder le premier run : `status = null` signifie
que l'attribut porte un autre nom dans le provider, à ajuster dans les deux outputs.

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
