# Migrations Alembic de `cos_service` : exécution en local

Ce dossier contient la configuration Alembic du schéma PostgreSQL `cos_service` :

| Fichier | Rôle |
|---|---|
| `../alembic.ini` | Configuration Alembic, à la racine du projet : `script_location = alembic`, `prepend_sys_path = .`, `sqlalchemy.url` vide |
| `env.py` | Charge `cos_service.models.base.Base` (`target_metadata`) et lit l'URL de la base dans la variable d'environnement `DB_URL` quand `sqlalchemy.url` est vide dans `alembic.ini` |
| `script.py.mako` | Gabarit des fichiers de révision |
| `versions/` | Les révisions, une par changement de schéma |

En production, les migrations sont appliquées par un DAG Airflow d'une autre
équipe, qui positionne `DB_URL` puis lance `alembic upgrade head`. La procédure
ci-dessous reproduit la même chose sur un poste macOS avec
[Postgres.app](https://postgresapp.com/).

Toutes les commandes se lancent depuis la racine du projet
(`~/PycharmProjects/cos`), avec le venv **complet** (`.venv`, créé par Poetry,
voir `docs/setup-poste-local.md`) : `env.py` importe les modèles, qui tirent
SQLAlchemy et la lib `bp2i`. Le venv de tests ne suffit pas.

---

## 1. Base PostgreSQL locale

Installer Postgres.app (glisser l'application dans `Applications`, pas de droits
admin nécessaires), l'ouvrir et cliquer sur **Initialize** si aucun serveur
n'existe encore. Postgres.app démarre un serveur « PostgreSQL 17 » sur le port 5432. Ne pas
créer un second serveur sur le même port : il ne démarrerait pas. Il suffit de
créer une base dans le serveur existant.

Postgres.app crée un rôle au nom de la session macOS (`h90871` dans les
exemples), sans mot de passe, en authentification locale `trust`.

```bash
export PATH="/Applications/Postgres.app/Contents/Versions/17/bin:$PATH"   # psql, createdb, dropdb
createdb cos_service
psql -d cos_service -c 'select version();'
```

Optionnel, pour utiliser le même rôle que le DAG plutôt que la session macOS :

```bash
psql -d postgres -c "create role cos with login password 'cos';"
psql -d postgres -c "alter database cos_service owner to cos;"
```

## 2. Préparer le shell

```bash
cd ~/PycharmProjects/cos
source .venv/bin/activate
python --version                                    # 3.12.x attendu, pas le 3.9 système
python -m pip install alembic psycopg2-binary       # ou : poetry add alembic psycopg2-binary
which alembic                                       # doit pointer sur .venv/bin/alembic

export PYTHONPATH="$PWD"          # rend cos_service.models.base importable depuis env.py (alembic.ini a aussi prepend_sys_path = .)

# rôle de la session macOS (Postgres.app, sans mot de passe) :
export DB_URL="postgresql+psycopg2://h90871@localhost:5432/cos_service"
# ou avec le rôle cos créé à l'étape 1 :
# export DB_URL="postgresql+psycopg2://cos:cos@localhost:5432/cos_service"
```

Pour ne pas retaper ces exports à chaque fois, les mettre dans un fichier
`.env.local` non versionné et le charger avec `source .env.local`.

## 3. Appliquer les migrations

```bash
python -m alembic heads            # doit afficher UNE seule tête (voir « Pièges »)
python -m alembic upgrade head     # applique toutes les révisions
python -m alembic current          # révision appliquée sur la base
psql -d cos_service -c '\dt'
```

## 4. Commandes du quotidien

```bash
python -m alembic history --verbose                              # chaîne des révisions
python -m alembic revision --autogenerate -m "description"       # nouvelle révision à partir du diff modèles / base
python -m alembic upgrade head                                   # l'appliquer
python -m alembic downgrade -1                                   # revenir d'un cran
python -m alembic upgrade head --sql > /tmp/migration.sql        # SQL seul, sans l'exécuter (revue)
```

Toujours relire une révision générée par `--autogenerate` avant de la
commiter : Alembic ne voit que les modèles importés par
`cos_service/models/__init__.py`. Un modèle non importé apparaît comme une
table à supprimer.

## 5. Repartir de zéro

```bash
dropdb cos_service && createdb cos_service && python -m alembic upgrade head
```

## Pièges

- **Deux têtes.** `versions/` contient deux révisions « init »
  (`afb71d26adf4_init_schema`, vide, et `de3255812781_init`). Si les deux ont
  `down_revision = None`, `alembic heads` en liste deux et `upgrade head`
  refuse. Supprimer la révision vide, ou la chaîner sur l'autre.
- **`ModuleNotFoundError: cos_service`** : `PYTHONPATH` n'est pas positionné,
  ou la commande n'est pas lancée depuis la racine du projet.
- **Traceback dans `/Users/<login>/Library/Python/3.9/...`** : le `alembic`
  lancé est celui du Python système 3.9, pas celui du venv. Installer Alembic
  dans `.venv` et lancer `python -m alembic ...`, qui ne peut pas se tromper
  d'interpréteur. `/usr/bin/python3 -m pip uninstall alembic` retire l'ancien.
- **`ModuleNotFoundError: No module named 'pytz'`** (ou un autre paquet
  importé par `cos_service/models/*.py`) : le venv n'a pas toutes les
  dépendances transitives d'Airflow. `python -m pip install pytz` suffit pour
  Alembic ; la vraie correction est de déclarer le paquet dans
  `pyproject.toml` (`poetry add pytz`).
- **`ModuleNotFoundError: bp2i_airflow_library`** : mauvais venv, activer
  `.venv` et non `.venv-tests`.
- **`KeyError: 'DB_URL'`** : la variable n'est pas exportée dans ce shell.
- **`connection refused`** : Postgres.app n'est pas démarré, ou le port n'est
  pas 5432.
- **Mot de passe dans les logs.** `env.py` affiche `sqlalchemy.url` avec un
  `print` avant de créer le moteur : à retirer, l'URL contient le mot de passe.

---

# Mise en conformité du schéma (octobre 2026) : ce qui a été fait et pourquoi

## Le point de départ

Le schéma PostgreSQL `cos` d'int, pprod et prod avait été construit par des
scripts SQL manuels. Alembic n'y avait tourné qu'une fois, avec une révision
vide (`de3255812781`, enregistrée dans `alembic_version`). Les modèles
SQLAlchemy de `cos_service/models/` décrivaient un schéma différent de la base
réelle : `varchar(n)` contre `text`, `NOT NULL` jamais posés, colonnes présentes
en base et absentes des modèles, et l'inverse.

Conséquence : chaque `alembic revision --autogenerate` produisait des dizaines
d'`ALTER COLUMN` que personne ne voulait, mêlés au vrai changement. Appliquées
en prod, ces opérations auraient modifié des types et des contraintes au hasard
d'une relecture oubliée.

## La démarche

1. **Copier la structure de pprod en local** (`cos_pprod`), à partir des scripts
   CREATE de pgAdmin, dans le schéma `cos` avec `search_path` positionné.
2. **Comparer** avec `alembic check` après un `stamp` sur la révision du schéma
   initial : la liste des écarts entre modèles et base.
3. **Corriger les modèles, pas la base** pour tout ce qui est cosmétique. La
   base de prod est la vérité : y faire des `ALTER COLUMN TYPE` n'apporte rien
   et peut échouer sur les données.
4. **Garder dans une révision dédiée** ce qui a une vraie valeur : un index, une
   clé étrangère, une conversion de type justifiée par les données.
5. **Répéter** jusqu'à `No new upgrade operations detected` sur la copie de
   pprod, après `upgrade head`.

## Ce qui a changé dans les modèles

| Changement | Pourquoi |
|---|---|
| Les neuf tables ont leur modèle dans le dépôt (`BackupVault`, `BackupVaultRestore`, `Bucket`, `BucketMigration`, `Context`, `Cos`, `CrossAccountServiceId`, `LifecyclePolicyRule`, `Workspace`), tous importés par `models/__init__.py` | Alembic ne voit que `Base.metadata` : un modèle non importé est une table « à supprimer » |
| Colonnes texte des tables historiques en `Text` | La base n'a jamais eu de `varchar(n)` ; `String(n)` dans le modèle générait des `ALTER COLUMN TYPE` à chaque autogenerate. `BackupVaultRestore` garde ses `String(n)`, sa table a bien des `varchar(n)` |
| `nullable=True` partout sauf là où la base impose `NOT NULL` (`bucket.virtual_server_endpoint`, les cinq booléens de `bucket` avec défaut `false`, `rule_id` / `status` / `expiration_days` de `lifecycle_policy_rule`) | Les scripts SQL n'avaient presque jamais posé de `NOT NULL`. SQLAlchemy ne vérifie pas `nullable` à l'insertion, seule la base le fait : aucun changement de comportement |
| `TIMESTAMP(timezone=True)` sur `backup_vault.updated_at` et `bucket.updated_at` | Ce sont des `timestamptz` en base |
| `bucket_migration.initial_cron_schedule_date_time` en `Text` | Stockée en texte en base |
| Ids de séquence en `bigint` (`BigId`, avec variante `INTEGER` sur SQLite pour les tests) | Les séquences de pprod sont en `bigint` |
| Colonnes présentes en base et oubliées des modèles : `bucket.bucket_etag`, `workspace.action_id`, `cos_instance.code_bl` / `code_bu` / `owership` / `payload`, `context.metier_n3` | Sinon Alembic propose de les supprimer. `owership` garde sa faute, c'est le nom en base |
| `BackupVaultRestore` : clé primaire `id` seul, `created_at` avec un défaut appelable, `zoneinfo` à la place de `pytz` | La base n'a jamais eu de clé composite ; l'ancien `default=datetime.now(...)` était figé à l'import du module ; `pytz` n'est pas dans `pyproject.toml` |
| Même ergonomie partout : constructeur par mots-clés, `to_dict()` construit depuis les colonnes (`_dict.row_to_dict`), `__iter__` qui le rejoue, relation absente = `None` | L'original avait des `to_dict` incomplets et des constructeurs à 20 arguments positionnels |

Tests : `tests/unit/models/test_bucket_model.py` charge les vrais modèles sur
SQLite, vérifie que les neuf tables sont déclarées, que les colonnes texte des
tables historiques sont en `Text`, et teste les aller-retours.

## La chaîne de révisions

```
<base> -> de3255812781           init (vide, déjà enregistrée dans les bases : ne jamais la supprimer)
       -> 68d62b4ffe7c           schéma initial : recrée tout sur une base vide ; se STAMPE sur les bases existantes
       -> 20261008_clean_grace   bucket : période de grâce du clean, unité de rétention (ADR 0001 / 0003)
       -> 20261029_restore_id    backup_vault_restore : id varchar -> bigint avec séquence, index, FK vers backup_vault
```

`afb71d26adf4_init_schema` et le fichier de lecture `0842d862feb2_drifting`
ont été supprimés : vides ou jamais appliqués nulle part.

## Validation obtenue en local

Sur la copie de pprod, dans cet ordre :

```bash
python -m alembic stamp --purge 68d62b4ffe7c
python -m alembic upgrade head          # 20261008 puis 20261029
python -m alembic check                 # No new upgrade operations detected
python -m alembic downgrade 68d62b4ffe7c && python -m alembic upgrade head
```

## Ce qu'il reste à faire avant la MR

1. Régénérer `68d62b4ffe7c_schema_initial.py` depuis une base vide avec les
   modèles corrigés, en gardant son identifiant (c'est lui que les bases
   connaîtront) :
   ```bash
   dropdb cos_service && createdb cos_service
   psql -d cos_service -c 'create schema cos;' -c 'alter database cos_service set search_path to cos;'
   export DB_URL="postgresql+psycopg2://h90871@localhost:5432/cos_service"
   rm alembic/versions/68d62b4ffe7c_schema_initial.py
   python -m alembic upgrade de3255812781
   python -m alembic revision --autogenerate -m "schema initial" --rev-id 68d62b4ffe7c
   ```
   Puis retirer à la main du fichier généré ce qui appartient aux révisions
   suivantes : les quatre colonnes `retention_unit` / `clean_*` de `bucket`,
   l'index `idx_restore_target_bucket`, la FK `backup_vault_restore ->
   backup_vault`, et remettre `backup_vault_restore.id` en `sa.String(50)`.
2. Retirer le `print` de l'URL dans `env.py` : il affiche le mot de passe.
3. Rejouer de zéro et vérifier : `dropdb` / `createdb`, `upgrade head`, `check` vide.

## Déploiement sur les environnements

Int, pprod et prod ont déjà les tables, et `alembic_version` pointe sur
`de3255812781`. Il ne faut pas exécuter `68d62b4ffe7c` mais la déclarer
appliquée, puis laisser le DAG `run_alembic` jouer les suivantes.

Pour chaque environnement, int d'abord, puis pprod, puis prod :

1. Déployer le code contenant les révisions.
2. Lancer les pré-contrôles de `20261029` (section `PRECHECK_SQL` du fichier) :
   ids non numériques, restaurations orphelines. Zéro ligne attendue.
3. Stamper, dans pgAdmin ou via `alembic stamp` :
   ```sql
   update cos.alembic_version set version_num = '68d62b4ffe7c';
   ```
4. Lancer `run_alembic` avec `head` depuis la console orchestrateur. Attendu
   dans les logs : `Running upgrade 68d62b4ffe7c -> 20261008_clean_grace`, puis
   `-> 20261029_restore_id`.
5. Vérifier : `select * from cos.alembic_version;` donne `20261029_restore_id`.

## Règles pour la suite

- Une révision générée par `--autogenerate` se **relit ligne par ligne** avant
  commit. Toute opération non voulue se supprime du fichier.
- Une révision enregistrée dans `alembic_version` d'un environnement ne se
  supprime jamais et ne change jamais d'identifiant.
- `alembic check` contre une copie de pprod avant chaque MR qui touche aux
  modèles : il doit être vide.
