# Tests unitaires `cos` sans les librairies internes

Ces tests chargent les DAGs et les services **sans** `bp2i_airflow_library`,
`bp2i_terraform`, Airflow ni les SDK IBM. La seule dépendance est `pytest`.

## Comment ça marche

- `conftest.py` met la racine du projet dans `sys.path` et installe des
  doublures (`tests/stubs/`) **uniquement** pour les modules qui ne s'importent
  pas. Sur un poste où les vraies libs sont installées, rien n'est remplacé.
- `stubs/bp2i.py` remplace les deux décorateurs du framework : `@step`
  enregistre la fonction Python brute de chaque étape au lieu de construire une
  tâche Airflow, et `depends(...)` vaut `None`. Chaque étape d'un DAG devient
  donc une fonction appelable directement.
- `stubs/orm.py` et `stubs/schemas.py` remplacent `sqlalchemy`, les modèles
  `cos_service.models.*` et les schémas absents.
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

# premier test : le DAG create déclare bien ses 6 étapes dans l'ordre
python3 -m pytest tests/dags/test_bucket_create.py::test_dag_declares_the_expected_steps_in_order -v

# tout le DAG create (39 tests), puis toute la suite
python3 -m pytest tests/dags/test_bucket_create.py -v
python3 -m pytest
```

Si `pip` est bloqué par le proxy, `pytest` est sans doute déjà présent dans le
venv du projet (`requirements-dev.txt`). Le test `compute_target_time` du DAG
update est ignoré (`skipped`) quand `pendulum` n'est pas installé.

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
