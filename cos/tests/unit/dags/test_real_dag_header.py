"""L'en-tête du VRAI cos.bucket.v1.create.py (celui du dépôt d'entreprise) doit
se charger sous les doublures : bloc de compatibilité Airflow 2/3, connecteur
reader, product_action(action_id=...). Le DAG reconstitué du dépôt n'a pas tous
ces imports ; ce test garantit que les doublures les couvrent quand même."""
import importlib.util
import sys
import textwrap

import pytest

from tests.unit.stubs import bp2i

REAL_HEADER = textwrap.dedent('''
    from enum import Enum
    from bp2i_airflow_library import add_project_to_path
    add_project_to_path()
    try:
        from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
    except ImportError:
        AIRFLOW_V_3_0_PLUS = False
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import Context as AirflowContext
        from airflow.sdk import TriggerRule, task, task_group
    else:
        from airflow.decorators import task, task_group  # noqa AIR301
        from airflow.utils.trigger_rule import TriggerRule  # noqa AIR301
        from airflow.utils.context import Context as AirflowContext  # noqa AIR301
    import logging
    from pathlib import Path
    from bp2i_airflow_library.config import ENVIRONMENT
    from bp2i_airflow_library.connectors.reader import ReaderConnector
    from bp2i_airflow_library.dag import product_action, step
    from bp2i_airflow_library.dependencies import (
        SASession, SchematicsBackend, StateManager, Vault, depends, payload_dependency,
        reader_dependency, smart_schematics_backend_dependency, sqlalchemy_session_dependency,
        state_manager_dependency, vault_dependency,
    )
    from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
    from bp2i_airflow_library.schemas import Field, ProductCreatePayload

    class BucketCreatePayload(ProductCreatePayload):
        storage_class: str = "standard"

    @product_action(
        action_id=Path(__file__).stem.replace(".v1.", ".v2.")
        if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
        else Path(__file__).stem,
        tags=["cos"],
        payload=BucketCreatePayload,
    )
    def bucket_create():
        @step
        def validate_request(
            payload: BucketCreatePayload = depends(payload_dependency),
            reader: ReaderConnector = depends(reader_dependency),
            session: SASession = depends(sqlalchemy_session_dependency),
            state_manager: StateManager = depends(state_manager_dependency),
        ) -> dict:
            return {"reader": reader}

        validate_request()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def airflow_task():
        return "ok"

    bucket_create()
''')


@pytest.fixture
def stubbed_modules(monkeypatch):
    modules = bp2i.build_modules()
    for name in bp2i.DAG_OVERRIDES:
        monkeypatch.setitem(sys.modules, name, modules[name])
    return modules


def test_real_dag_header_loads_under_the_stubs(stubbed_modules, tmp_path):
    dag_file = tmp_path / "cos.bucket.v1.create.py"
    dag_file.write_text(REAL_HEADER)
    spec = importlib.util.spec_from_file_location("real_header_dag", dag_file)
    module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(module)

    assert module.AIRFLOW_V_3_0_PLUS is False
    assert module.BucketCreatePayload(realm="r").storage_class == "standard"
    assert module.bucket_create.dag_name == "cos.bucket.v1.create"
    assert list(stubbed_modules["bp2i_airflow_library.dag"].registry) == ["validate_request"]
    assert module.airflow_task() == "ok"


def test_airflow_decorators_accept_both_call_styles(stubbed_modules):
    decorators = stubbed_modules["airflow.decorators"]

    @decorators.task
    def bare():
        return 1

    @decorators.task(retries=2)
    def with_args():
        return 2

    @decorators.task_group(group_id="g")
    def group():
        return 3

    assert (bare(), with_args(), group()) == (1, 2, 3)
    assert stubbed_modules["airflow.utils.trigger_rule"].TriggerRule.ALL_DONE == "all_done"


def test_product_action_keeps_the_positional_form(stubbed_modules):
    dag = stubbed_modules["bp2i_airflow_library.dag"]

    @dag.product_action("cos.bucket.v1.delete", tags=["cos"])
    def legacy():
        pass

    assert legacy.dag_name == "cos.bucket.v1.delete"
