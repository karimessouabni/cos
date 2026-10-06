"""Tests de ``quarantine_service`` : la quarantaine est un apply du workspace du bucket
avec la variable Terraform ``quarantine``."""
import sys
from unittest.mock import MagicMock

import pytest

from cos_service.services import quarantine_service as svc

BUCKET = {
    "subscription_id": "sub-1",
    "name": "bucket-a",
    "region": "eu-de",
    "storage_class": "standard",
    "activity_tracker_crn": "crn:logs",
    "kms_crn": "crn:kms",
    "monitoring_crn": "crn:logs",
    "enable_custom_permissions": False,
    "backup_vault": None,
    "workspace": {"workspace_id": "ws-1"},
    "cos": {"crn": "crn:cos", "name": "cos-a", "context": {"realm": "realm-a", "app_code": "AP1"}},
}


@pytest.fixture
def deps(monkeypatch):
    mocks = {}
    for name in ("workspaceService", "schematics_service", "immutability_service"):
        mocks[name] = MagicMock(name=name)
        monkeypatch.setitem(sys.modules, f"cos_service.services.{name}", mocks[name])
    mocks["immutability_service"].compute_bucket_immutability_for_update_bucket.return_value = {"immutability_choice": "none"}
    mocks["schematics_service"].run_workspace.return_value = {"bucket_crn": {"value": "crn:bucket"}}
    return mocks


def run(enabled, bucket=BUCKET, tf=None):
    return svc.set_bucket_quarantine(
        bucket=bucket, enabled=enabled, payload="payload", description="my bucket",
        tf=tf or "tf", vault="vault", reader="reader", session="session",
    )


@pytest.mark.parametrize("enabled", [True, False])
def test_rebuilds_the_workspace_details_with_the_quarantine_flag_then_applies(deps, enabled):
    assert run(enabled) == {"bucket_crn": {"value": "crn:bucket"}}

    details = deps["workspaceService"].build_bucket_workspace_details.call_args.kwargs
    assert details["quarantine"] is enabled
    assert details["workspace_id"] == "ws-1"
    assert details["realm"] == "realm-a"
    assert details["cos_instance_crn"] == "crn:cos"
    assert details["management_endpoint_type"] == "direct"
    assert details["description"] == "my bucket"
    assert details["backup_vault_crn"] is None
    assert details["immutability"] == {"immutability_choice": "none"}
    deps["immutability_service"].compute_bucket_immutability_for_update_bucket.assert_called_once_with(BUCKET, "session")
    deps["workspaceService"].update_bucket_workspace.assert_called_once_with(
        payload="payload", workspace_details=deps["workspaceService"].build_bucket_workspace_details.return_value,
        vault="vault", tf="tf", reader="reader",
    )
    deps["schematics_service"].run_workspace.assert_called_once_with("tf", "ws-1")


def test_backup_vault_crn_comes_from_the_bucket_relation(deps):
    run(True, bucket={**BUCKET, "backup_vault": {"crn": "crn:bv"}})

    assert deps["workspaceService"].build_bucket_workspace_details.call_args.kwargs["backup_vault_crn"] == "crn:bv"


def test_apply_failure_propagates_without_a_run(deps):
    deps["workspaceService"].update_bucket_workspace.side_effect = RuntimeError("schematics down")

    with pytest.raises(RuntimeError, match="schematics down"):
        run(True)

    deps["schematics_service"].run_workspace.assert_not_called()
