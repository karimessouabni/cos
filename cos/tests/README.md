# Tests unitaires `cos` sans les librairies internes

Ces tests chargent les DAGs et les services **sans** `bp2i_airflow_library`,
`bp2i_terraform`, Airflow ni les SDK IBM. La seule dépendance est `pytest`.

## Comment ça marche

- `conftest.py` met la racine du projet dans `sys.path` et installe des
  doublures (`tests/stubs/`) à deux niveaux :
  - **toujours**, pour la frontière d'infrastructure dont les tests unitaires
    dépendent : `bp2i_airflow_library`, `bp2i_terraform`, `airflow`,
    `sqlalchemy` et les modèles `cos_service.models.*`. Même avec le venv
    complet, un test unitaire ne parle pas à une base ni à Airflow ;
  - **seulement si le vrai module manque**, pour le code projet :
    `cos_service.schemas.*`, `cos_service.utils.*`, `cos_service.repository.*`.
    Sur le venv complet, ce sont les vrais schémas et constantes.
  - `COS_TESTS_FORCE_STUBS=0` désactive le premier niveau (tout ce qui existe
    reste réel) : utile pour `tests/integration`, pas pour les unitaires.
- Les services et les étapes de DAG testés sont toujours le vrai code.
- `stubs/bp2i.py` remplace les deux décorateurs du framework : `@step`
  enregistre la fonction Python brute de chaque étape au lieu de construire une
  tâche Airflow, et `depends(...)` vaut `None`. Chaque étape d'un DAG devient
  donc une fonction appelable directement.
- `stubs/orm.py` et `stubs/schemas.py` remplacent `sqlalchemy`, les modèles
  `cos_service.models.*` et les schémas absents (`bucket_retention` est présent
  dans le dépôt et n'est plus doublé ; il demande `pydantic`, voir
  `requirements-test.txt`).
- La fixture `services` remplace chaque `cos_service.services.*` par un
  `MagicMock`, sauf `immutability_service` qui reste le vrai module.
- La fixture `dag` charge `cos.bucket.v1.create.py` et expose
  `dag.steps["<nom_de_l_etape>"]` et `dag.module`.

## Lancer le premier test

Sur le poste, le projet est à plat (`~/PycharmProjects/cos/cos_service`,
`~/PycharmProjects/cos/tests`). Dans ce dépôt GitHub, il y a un niveau `cos/`
en plus : les commandes ci-dessous se lancent depuis le dossier qui contient
`cos_service/` et `tests/`.

```bash
cd ~/PycharmProjects/cos
python3 -m venv .venv-tests && source .venv-tests/bin/activate   # optionnel
python3 -m pip install -r requirements-test.txt                  # = pytest

# premier test : trois cas simples sur le DAG create
python3 -m pytest tests/dags/test_bucket_create_smoke.py -v

# tout le DAG create (39 tests par étape), puis les 7 scénarios bout-en-bout, puis toute la suite
python3 -m pytest tests/dags/test_bucket_create.py -v
python3 -m pytest tests/dags/test_bucket_create_scenario.py -v
python3 -m pytest
```

`test_bucket_create_scenario.py` enchaîne les six étapes dans l'ordre du DAG
avec `run_create_dag()` : chemin nominal, backup vault, workspace existant,
demande déclinée, échec Terraform. C'est le modèle à copier pour les autres DAGs.

Si `pip` est bloqué par le proxy, `pytest` est sans doute déjà présent dans le
venv du projet (`requirements-dev.txt`). Le test `compute_target_time` du DAG
update est ignoré (`skipped`) quand `pendulum` n'est pas installé.

## Couverture de tests

En ligne de commande, avec `pytest-cov` (dans `requirements-test.txt`) et la
configuration de `.coveragerc` (source `cos_service/`, branches comprises) :

```bash
python3 -m pytest --cov --cov-report=term-missing      # tableau + lignes manquantes
python3 -m pytest --cov --cov-report=html              # rapport navigable : htmlcov/index.html
python3 -m pytest --cov --cov-report=xml               # coverage.xml pour GitLab CI / SonarQube
```

Les DAGs chargés par chemin par la fixture `dag` sont bien comptés. Point de
départ mesuré sur ce dépôt : 92,6 % sur `cos_service/`, les 7,4 % restants
étant surtout le DAG restore. Pour bloquer la CI sous un seuil, ajouter
`--cov-fail-under=90` (partir du niveau mesuré, puis remonter).

Dans PyCharm : clic droit sur `tests/` ou sur un fichier de test, puis
« Run 'pytest in tests' with Coverage » (icône bouclier à côté du bouton Run).
Le pourcentage s'affiche par dossier dans l'explorateur de projet et les
lignes non couvertes sont surlignées en rouge dans l'éditeur. PyCharm utilise
son propre runner de couverture : ne pas mettre `--cov` dans `addopts` de
`pytest.ini`, les deux entreraient en conflit.

## Avec le venv complet (lib bp2i, Airflow, SQLAlchemy installés)

Les unitaires se lancent pareil, `python3 -m pytest`, et restent isolés de
l'infrastructure. En plus, `tests/integration/test_dag_integrity.py` charge
les DAGs avec le **vrai** framework via le `DagBag` Airflow :

```bash
COS_TESTS_FORCE_STUBS=0 python3 -m pytest -m integration tests/integration -v
```

Il est exclu du run par défaut (`-m "not integration"` dans `pytest.ini`) et
s'ignore tout seul si Airflow ou la lib manquent. La lib lit `ENVIRONMENT` et
`DEFAULT_PRODUCT_BRANCH` dans l'environnement dès son import : le test les
positionne à `int` et `main` s'ils sont absents. Si le framework bp2i lit
des Variables ou Connections Airflow au parsing, ce test le montrera : c'est
le premier point à vérifier avant de le mettre en CI.

## Tests de DAG contre la vraie lib (expérimental)

Avec `COS_TESTS_FORCE_STUBS=0` et la lib importable, la fixture `load_dag`
n'utilise plus les doublures : le vrai `product_action` construit le DAG
Airflow, puis chaque étape est retrouvée dans `dag.tasks` via `python_callable`,
déballé de ses wrappers (`__wrapped__`). Les mêmes tests tournent alors sur
les vrais décorateurs :

```bash
COS_TESTS_FORCE_STUBS=0 python -m pytest tests/dags/test_bucket_create.py -v
```

Deux échecs possibles, tous deux explicites dans le message d'erreur :
- `product_action n'a construit aucun DAG` : le décorateur ne passe pas par
  `DAG(...)` de `version_compat` ni ne dépose le DAG dans les globals ;
- `aucune étape retrouvée` : le wrapper de `@step` n'expose pas `__wrapped__`.
  Il faut alors adapter `_unwrap()` dans `conftest.py` au wrapper réel.

Le test `test_dag_declares_the_expected_steps_in_order` ne compte que les
étapes définies dans le fichier du DAG ; les tâches ajoutées par le framework
(bootstrap, notify...) sont listées dans `dag.framework_tasks`.

## Écrire un test d'étape

```python
from unittest.mock import MagicMock

import pytest
from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException


def test_unknown_realm_is_declined(dag, make_payload, services, state_manager):
    services.contextService.get_realm.return_value = None

    with pytest.raises(DeclineDemandException):
        dag.steps["validate_request"](
            payload=make_payload(realm="nope"),
            session=MagicMock(),
            state_manager=state_manager,
        )
```

Les dépendances déclarées avec `depends(...)` dans le DAG (`payload`, `session`,
`state_manager`, `tf`, `vault`) sont passées explicitement en kwargs.

## Si un test échoue sur une assertion et pas sur un import

Les DAGs de ce dépôt ont été reconstitués depuis des captures ; le DAG local a
pu évoluer depuis. Un échec d'assertion indique précisément l'écart entre le
DAG réel et le comportement attendu par le test : c'est le test ou le DAG à
ajuster, pas le harnais.
