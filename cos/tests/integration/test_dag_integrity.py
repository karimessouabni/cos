"""Intégrité des DAGs avec le VRAI framework : chargement par le DagBag Airflow.

Nécessite le venv complet (Airflow + bp2i_airflow_library) et les doublures
désactivées :

    COS_TESTS_FORCE_STUBS=0 python -m pytest -m integration tests/integration -v

Ignoré automatiquement quand Airflow ou la lib manquent, ou quand les
doublures sont actives.
"""
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

DAGS_DIR = Path(__file__).resolve().parents[2] / "cos_service" / "dags"


def _real_module_or_skip(name: str):
    """importorskip qui refuse les doublures de tests/stubs (elles n'ont pas de __file__)."""
    module = pytest.importorskip(name, reason=f"{name} absent")
    if not getattr(module, "__file__", None):
        pytest.skip(f"{name} est une doublure de tests/stubs : venv complet requis")
    return module


# bp2i_airflow_library/config/environment.py lit ces variables à l'import
# (os.environ[...] sans défaut) : sans elles, importer la lib lève KeyError.
LIB_ENV_DEFAULTS = {
    "ENVIRONMENT": "int",
    "DEFAULT_PRODUCT_BRANCH": "main",
    "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
}


@pytest.fixture(scope="module")
def dagbag():
    if os.environ.get("COS_TESTS_FORCE_STUBS", "1") != "0":
        pytest.skip("doublures actives : relancer avec COS_TESTS_FORCE_STUBS=0")
    for key, value in LIB_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)
    _real_module_or_skip("bp2i_airflow_library")
    _real_module_or_skip("airflow")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_all_dag_files_import_without_error(dagbag):
    assert dagbag.import_errors == {}, "\n".join(f"{k}: {v}" for k, v in dagbag.import_errors.items())


def test_at_least_one_dag_is_registered(dagbag):
    assert dagbag.dag_ids, f"aucun DAG trouvé sous {DAGS_DIR}"


@pytest.mark.parametrize("expected", ["cos.bucket.v1.create"])
def test_expected_dag_is_present(dagbag, expected):
    matches = [dag_id for dag_id in dagbag.dag_ids if expected in dag_id]
    assert matches, f"{expected} absent de {sorted(dagbag.dag_ids)}"


def test_dags_have_no_cycle_and_a_finite_task_count(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tasks, f"{dag_id} sans tâche"
        dag.check_cycle() if hasattr(dag, "check_cycle") else None
