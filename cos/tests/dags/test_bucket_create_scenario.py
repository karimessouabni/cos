"""Scénarios bout-en-bout du DAG ``cos.bucket.v1.create``.

Là où ``test_bucket_create.py`` teste chaque étape isolément, ce fichier
enchaîne les six étapes dans l'ordre du câblage du DAG, en passant la sortie
de chacune à la suivante exactement comme le fait ``bucket_create()`` :

    validate_request
      -> process_protection_configuration(validated)
      -> get_account_instances_crn(validated)
      -> create_tf_workspace(validated, account_instances_crn, immutability)
      -> apply_tf_workspace(validated, workspace_id, account_instances_crn, immutability)
      -> save_bucket_in_db(apply_tf_result, immutability)

Les services ``cos_service.services.*`` sont des ``MagicMock`` (fixture
``happy_services``), ``immutability_service`` est le vrai module. Aucune lib
interne, Airflow ni base de données n'est nécessaire : voir ``tests/README.md``.

Lancement :

    python -m pytest tests/dags/test_bucket_create_scenario.py -v
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.exceptions.flow_control import DeclineDemandException
from cos_service.schemas.bucket_backup import BucketBackup
from cos_service.schemas.immutability import Immutability
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus

# Fixtures et constantes partagées avec les tests par étape.
from tests.dags.test_bucket_create import (  # noqa: F401 - fixtures importées pour pytest
    ACCOUNT_CRNS,
    BACKUP_VAULT,
    COS_INSTANCE,
    REALM,
    TF_OUTPUTS,
    happy_services,
    make_payload,
    state_manager,
)

SESSION = "session"
TF = "tf"
VAULT = "vault"


def run_create_dag(dag, payload, state_manager, session=SESSION, tf=TF, vault=VAULT) -> SimpleNamespace:
    """Exécute les six étapes dans l'ordre du DAG et renvoie leurs sorties."""
    steps = dag.steps
    validated = steps["validate_request"](payload=payload, session=session, state_manager=state_manager)
    immutability = steps["process_protection_configuration"](validated=validated, payload=payload)
    account_instances_crn = steps["get_account_instances_crn"](validated=validated)
    workspace_id = steps["create_tf_workspace"](
        validated=validated,
        account_instances_crn=account_instances_crn,
        immutability=immutability,
        tf=tf,
        state_manager=state_manager,
        session=session,
        payload=payload,
        vault=vault,
    )
    apply_tf_result = steps["apply_tf_workspace"](
        validated=validated,
        workspace_id=workspace_id,
        account_instances_crn=account_instances_crn,
        immutability=immutability,
        tf=tf,
        state_manager=state_manager,
        session=session,
        payload=payload,
        vault=vault,
    )
    result = steps["save_bucket_in_db"](
        apply_tf_result=apply_tf_result,
        immutability=immutability,
        payload=payload,
        state_manager=state_manager,
        session=session,
    )
    return SimpleNamespace(
        validated=validated,
        immutability=immutability,
        account_instances_crn=account_instances_crn,
        workspace_id=workspace_id,
        apply_tf_result=apply_tf_result,
        result=result,
    )


@pytest.fixture
def infra(happy_services):
    """Réponses des services pour une création qui va au bout : bucket inexistant,
    workspace Terraform créé puis appliqué avec ses outputs."""
    happy_services.bucketService.get_bucket_by_sub_id.return_value = None
    happy_services.bucketService.process_bucket_creation.return_value = {"workspace": {"workspace_id": None}}
    happy_services.schematics_service.create_or_update_ws.return_value = {"id": "ws-1"}
    happy_services.schematics_service.run_workspace.return_value = TF_OUTPUTS
    return happy_services


def pushed_states(state_manager) -> list[dict]:
    return [call.args[0] for call in state_manager.push_state.call_args_list]


# --- scénario nominal -----------------------------------------------------------

def test_happy_path_creates_the_bucket_end_to_end(dag, infra, make_payload, state_manager):
    payload = make_payload()

    run = run_create_dag(dag, payload, state_manager)

    # chaque étape a transmis le bon objet à la suivante
    assert run.validated == {"realm": REALM, "cos_instance": dict(COS_INSTANCE), "backup_vault": None}
    assert run.immutability["immutability_choice"] == Immutability.NONE.value
    assert run.account_instances_crn == ACCOUNT_CRNS
    assert run.workspace_id == "ws-1"
    assert run.apply_tf_result == TF_OUTPUTS
    assert run.result == {"subscription_id": "sub-1"}

    # l'état publié au fil du DAG : instance COS, workspace, puis bucket final
    states = pushed_states(state_manager)
    assert states[0] == {"cos_instance": "co21000001"}
    assert states[1] == {"workspace_name": "ws_bucket_sub-1"}
    final = states[2]
    assert final["name"] == "bucket-a"
    assert final["crn"] == "crn:bucket"
    assert final["virtual_server_endpoint"] == {
        "host_style": "https://bucket-a.s3.direct.eu-de.cloud-object-storage.appdomain.cloud",
    }
    assert final["clean_status"] == Status.SUCCESS.value
    assert final["immutability_choice"] == Immutability.NONE.value
    assert final["enable_versioning"] is False
    assert final["backup"]["backup_enabled"] is False
    assert len(states) == 3

    # Terraform a reçu les variables issues de la validation, pas d'une relecture
    variables = infra.schematics_service.create_or_update_ws.call_args.kwargs["variables"]
    assert variables["cos_instance_crn"] == "crn:cos"
    assert variables["cos_instance_name"] == "cos-a"
    assert variables["kms_key_crn"] == "crn:kms"
    assert variables["wklapp_account_id"] == "wk-123"
    assert variables["target_backup_vault_crn"] is None
    assert variables["cloud_type"] == "3"
    infra.schematics_service.run_workspace.assert_called_once_with(TF, "ws-1")

    # les statuts du bucket suivent le cycle CREATING -> CREATING, workspace INPROGRESS
    assert infra.bucketService.update_bucket_status.call_args_list == [
        ((payload.subscription_id, SubscriptionStatus.CREATING, SESSION),),
        ((payload.subscription_id, SubscriptionStatus.CREATING, SESSION),),
    ]
    infra.bucketService.update_bucket_workspace_status.assert_called_once_with(
        payload.subscription_id, Status.INPROGRESS, SESSION
    )
    infra.bucketService.complete_bucket_create.assert_called_once_with(
        payload.subscription_id,
        "https://s3.direct.eu-de.cloud-object-storage.appdomain.cloud/bucket-a",
        TF_OUTPUTS,
        SESSION,
    )
    # aucune mise en LOCKED sur le chemin nominal
    assert all(
        call.args[1] != SubscriptionStatus.LOCKED for call in infra.bucketService.update_bucket_status.call_args_list
    )


def test_backup_vault_flows_from_validation_to_terraform_and_state(dag, infra, make_payload, state_manager):
    # le backup exige le versioning et une rétention, sinon la demande est déclinée
    backup = BucketBackup(backup_enabled=True, backup_vault_name="bv-a", backup_retention_days=7)
    payload = make_payload(enable_versioning=True, backup=backup)

    run = run_create_dag(dag, payload, state_manager)

    assert run.validated["backup_vault"] == BACKUP_VAULT
    assert run.immutability["backup"] == {
        "backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7
    }
    assert run.immutability["object_versioning_enabled"] is True

    variables = infra.schematics_service.create_or_update_ws.call_args.kwargs["variables"]
    assert variables["backup_enabled"] is True
    assert variables["target_backup_vault_crn"] == "crn:bv"

    # la ligne bucket est créée avec la ligne ORM du vault, résolue une seule fois
    infra.backup_vault_service.get_backup_vault_by_sub_id.assert_called_once_with("bv-sub", SESSION)
    infra.backup_vault_service.get_backup_vault_by_name.assert_called_once()

    details = infra.workspaceService.build_bucket_workspace_details.call_args.kwargs
    assert details["backup_vault_crn"] == "crn:bv"
    assert pushed_states(state_manager)[-1]["backup"] == run.immutability["backup"]


def test_existing_workspace_is_reused_without_recreating_terraform(dag, infra, make_payload, state_manager):
    infra.bucketService.get_bucket_by_sub_id.return_value = {"workspace": {"workspace_id": "ws-existing"}}

    run = run_create_dag(dag, make_payload(), state_manager)

    assert run.workspace_id == "ws-existing"
    infra.bucketService.process_bucket_creation.assert_not_called()
    infra.schematics_service.create_or_update_ws.assert_not_called()
    infra.schematics_service.run_workspace.assert_called_once_with(TF, "ws-existing")
    assert run.result == {"subscription_id": "sub-1"}


# --- scénarios d'échec ----------------------------------------------------------

def test_declined_request_stops_before_any_side_effect(dag, infra, make_payload, state_manager):
    infra.contextService.get_realm.return_value = None

    with pytest.raises(DeclineDemandException) as excinfo:
        run_create_dag(dag, make_payload(), state_manager)

    assert "the realm realm-a doesn't exist" in str(excinfo.value)
    # validate_request publie l'instance COS avant de décliner : c'est le seul état émis
    assert pushed_states(state_manager) == [{"cos_instance": "co21000001"}]
    infra.bucketService.process_bucket_creation.assert_not_called()
    infra.schematics_service.create_or_update_ws.assert_not_called()
    infra.bucketService.update_bucket_status.assert_not_called()


def test_object_lock_without_versioning_is_declined_after_validation(dag, infra, make_payload, state_manager):
    payload = make_payload(immutability_choice=Immutability.OBJECT_LOCK, enable_versioning=False)

    with pytest.raises(DeclineDemandException):
        run_create_dag(dag, payload, state_manager)

    # validate_request est passée (l'instance COS a été publiée), rien après
    assert pushed_states(state_manager) == [{"cos_instance": "co21000001"}]
    infra.schematics_service.create_or_update_ws.assert_not_called()


def test_terraform_apply_failure_locks_the_bucket_and_skips_the_save(dag, infra, make_payload, state_manager):
    infra.schematics_service.run_workspace.side_effect = RuntimeError("schematics apply failed")
    payload = make_payload()

    with pytest.raises(RuntimeError, match="schematics apply failed"):
        run_create_dag(dag, payload, state_manager)

    assert infra.bucketService.update_bucket_status.call_args_list[-1].args == (
        payload.subscription_id, SubscriptionStatus.LOCKED, SESSION,
    )
    assert infra.bucketService.update_bucket_workspace_status.call_args_list[-1].args == (
        payload.subscription_id, Status.FAILED, SESSION,
    )
    infra.bucketService.complete_bucket_create.assert_not_called()
    # le workspace a bien été créé avant l'échec de l'apply
    assert pushed_states(state_manager) == [{"cos_instance": "co21000001"}, {"workspace_name": "ws_bucket_sub-1"}]


def test_workspace_creation_failure_locks_the_bucket(dag, infra, make_payload, state_manager):
    infra.schematics_service.create_or_update_ws.side_effect = RuntimeError("schematics create failed")
    payload = make_payload()

    with pytest.raises(RuntimeError, match="schematics create failed"):
        run_create_dag(dag, payload, state_manager)

    infra.bucketService.update_bucket_status.assert_called_with(payload.subscription_id, SubscriptionStatus.LOCKED, SESSION)
    infra.bucketService.update_bucket_workspace_status.assert_called_with(payload.subscription_id, Status.FAILED, SESSION)
    infra.schematics_service.run_workspace.assert_not_called()
    infra.bucketService.complete_bucket_create.assert_not_called()
