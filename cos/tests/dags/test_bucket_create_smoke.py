"""Premier test du DAG ``cos.bucket.v1.create`` : trois cas simples.

Lancement :

    python -m pytest tests/dags/test_bucket_create_smoke.py -v

Ce qu'il faut savoir, et rien de plus :

- ``dag`` (fixture de ``tests/conftest.py``) charge le fichier du DAG et expose
  chaque étape comme une fonction Python : ``dag.steps["validate_request"]`` ;
- ``happy_services`` remplace les services (``contextService``, ``cosService``...)
  par des ``MagicMock`` déjà configurés pour une demande valide ;
- ``make_payload()`` fabrique un payload valide, ``make_payload(realm="x")``
  en change un champ ;
- ``state_manager`` est un ``MagicMock`` : on vérifie ce que le DAG lui publie.
"""
import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException

from tests.dags.test_bucket_create import (  # noqa: F401 - fixtures réutilisées
    REALM,
    happy_services,
    make_payload,
    state_manager,
)


def test_le_dag_declare_six_etapes_dans_l_ordre(dag):
    assert list(dag.steps) == [
        "validate_request",
        "process_protection_configuration",
        "get_account_instances_crn",
        "create_tf_workspace",
        "apply_tf_workspace",
        "save_bucket_in_db",
    ]


def test_une_demande_valide_passe_la_validation(dag, happy_services, make_payload, state_manager):
    result = dag.steps["validate_request"](
        payload=make_payload(),
        session="session",
        state_manager=state_manager,
    )

    assert result["realm"] == REALM
    assert result["cos_instance"]["name"] == "cos-a"
    assert result["backup_vault"] is None
    state_manager.push_state.assert_called_once_with({"cos_instance": "co21000001"})


def test_un_realm_inconnu_est_decline(dag, happy_services, make_payload, state_manager):
    happy_services.contextService.get_realm.return_value = None

    with pytest.raises(DeclineDemandException) as excinfo:
        dag.steps["validate_request"](
            payload=make_payload(realm="realm-a"),
            session="session",
            state_manager=state_manager,
        )

    assert str(excinfo.value) == "the realm realm-a doesn't exist"
