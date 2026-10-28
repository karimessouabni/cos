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
- **`ModuleNotFoundError: bp2i_airflow_library`** : mauvais venv, activer
  `.venv` et non `.venv-tests`.
- **`KeyError: 'DB_URL'`** : la variable n'est pas exportée dans ce shell.
- **`connection refused`** : Postgres.app n'est pas démarré, ou le port n'est
  pas 5432.
- **Mot de passe dans les logs.** `env.py` affiche `sqlalchemy.url` avec un
  `print` avant de créer le moteur : à retirer, l'URL contient le mot de passe.
