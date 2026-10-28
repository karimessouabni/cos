# Migrations Alembic de `cos_service` : exécution en local

Ce dossier contient la configuration Alembic du schéma PostgreSQL `cos_service` :

| Fichier | Rôle |
|---|---|
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

Postgres.app démarre un serveur « PostgreSQL 17 » sur le port 5432. Ne pas
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

Sans Postgres.app, un conteneur fait le même travail :

```bash
docker run -d --name cos-pg -p 5432:5432 \
  -e POSTGRES_USER=cos -e POSTGRES_PASSWORD=cos -e POSTGRES_DB=cos_service \
  postgres:17
```

## 2. Préparer le shell

```bash
cd ~/PycharmProjects/cos
source .venv/bin/activate
pip show alembic psycopg2-binary >/dev/null || pip install alembic psycopg2-binary

export PYTHONPATH="$PWD"          # rend cos_service.models.base importable depuis env.py

# rôle de la session macOS (Postgres.app, sans mot de passe) :
export DB_URL="postgresql+psycopg2://h90871@localhost:5432/cos_service"
# ou rôle cos / docker :
# export DB_URL="postgresql+psycopg2://cos:cos@localhost:5432/cos_service"
```

## 3. Appliquer les migrations

```bash
alembic heads            # doit afficher UNE seule tête (voir « Pièges »)
alembic upgrade head     # applique toutes les révisions
alembic current          # révision appliquée sur la base
psql -d cos_service -c '\dt'
```

## 4. Commandes du quotidien

```bash
alembic history --verbose                              # chaîne des révisions
alembic revision --autogenerate -m "description"       # nouvelle révision à partir du diff modèles / base
alembic upgrade head                                   # l'appliquer
alembic downgrade -1                                   # revenir d'un cran
alembic upgrade head --sql > /tmp/migration.sql        # SQL seul, sans l'exécuter (revue)
```

Toujours relire une révision générée par `--autogenerate` avant de la
commiter : Alembic ne voit que les modèles importés par
`cos_service/models/__init__.py`. Un modèle non importé apparaît comme une
table à supprimer.

## 5. Repartir de zéro

```bash
dropdb cos_service && createdb cos_service && alembic upgrade head
```

## Pièges

- **Deux têtes.** `versions/` contient deux révisions « init »
  (`afb71d26adf4_init_schema`, vide, et `de3255812781_init`). Si les deux ont
  `down_revision = None`, `alembic heads` en liste deux et `upgrade head`
  refuse. Supprimer la révision vide, ou la chaîner sur l'autre.
- **`ModuleNotFoundError: cos_service`** : `PYTHONPATH` n'est pas positionné,
  ou la commande n'est pas lancée depuis la racine du projet.
- **`ModuleNotFoundError: bp2i_airflow_library`** : mauvais venv, activer
  `.venv` et non `.venv-tests`.
- **`KeyError: 'DB_URL'`** : la variable n'est pas exportée dans ce shell.
- **`connection refused`** : Postgres.app n'est pas démarré, ou le port n'est
  pas 5432.
- **Mot de passe dans les logs.** `env.py` affiche `sqlalchemy.url` avec un
  `print` avant de créer le moteur : à retirer, l'URL contient le mot de passe.
