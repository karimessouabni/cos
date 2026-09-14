"""Tests de bucketService : session SQLAlchemy et ``requests`` mockés."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cos_service.models.Bucket import Bucket
from cos_service.models.Workspace import Workspace
from cos_service.schemas.action import Action
from cos_service.schemas.status import Status
from cos_service.schemas.subscription_status import SubscriptionStatus
from cos_service.services import bucketService as svc

S3 = "http://s3.amazonaws.com/doc/2006-03-01/"
BUCKET = {"cos": {"crn": "crn:cos"}, "virtual_server_endpoint": "https://s3.direct.eu-de.example/bucket-a"}


def xml_response(body: str, status_code: int = 200) -> MagicMock:
    response = MagicMock(status_code=status_code, content=body.encode("utf-8"), text=body)
    if status_code >= 400:
        response.raise_for_status.side_effect = RuntimeError(f"HTTP {status_code}")
    return response


@pytest.fixture
def session():
    return MagicMock(name="session")


@pytest.fixture
def http(monkeypatch):
    requests = MagicMock(name="requests")
    monkeypatch.setattr(svc, "requests", requests)
    return requests


def executed(session) -> list:
    return [call.args[0] for call in session.execute.call_args_list]


# --- lecture ------------------------------------------------------------------

class TestGetBucketBySubId:
    def query(self, session):
        return session.query.return_value.options.return_value

    def test_exact_match_returns_a_dict(self, session):
        row = Bucket(subscription_id="sub-1", name="bucket-a")
        self.query(session).filter.return_value.one_or_none.return_value = row

        assert svc.get_bucket_by_sub_id(session, "sub-1") == {"subscription_id": "sub-1", "name": "bucket-a"}
        condition = self.query(session).filter.call_args.args[0]
        assert condition == ("==", "subscription_id", "sub-1")

    def test_relations_read_by_the_dags_are_eager_loaded(self, session):
        self.query(session).filter.return_value.one_or_none.return_value = None

        svc.get_bucket_by_sub_id(session, "sub-1")

        loads = session.query.return_value.options.call_args.args
        chains = [[column.name for column in load.chain] for load in loads]
        assert ["workspace"] in chains
        assert ["cos", "workspace"] in chains
        assert ["cos", "context"] in chains
        assert ["backup_vault"] in chains

    def test_missing_bucket_is_none(self, session):
        self.query(session).filter.return_value.one_or_none.return_value = None

        assert svc.get_bucket_by_sub_id(session, "sub-1") is None


class TestGetBucketWorkspace:
    def test_reads_the_workspace_table_by_bucket_subscription_id(self, session):
        row = Workspace(workspace_id="ws-1", bucket_subscription_id="sub-1")
        session.query.return_value.filter.return_value.one_or_none.return_value = row

        assert svc.get_bucket_workspace(session, "sub-1") == {"workspace_id": "ws-1", "bucket_subscription_id": "sub-1"}
        session.query.assert_called_once_with(Workspace)
        condition = session.query.return_value.filter.call_args.args[0]
        assert condition == ("==", "bucket_subscription_id", "sub-1")

    def test_missing_workspace_is_none(self, session):
        session.query.return_value.filter.return_value.one_or_none.return_value = None

        assert svc.get_bucket_workspace(session, "sub-1") is None


# --- appels S3 ---------------------------------------------------------------------

class TestCheckBucketHasContents:
    def test_one_key_is_enough(self, http):
        http.get.return_value = xml_response(
            f'<ListBucketResult xmlns="{S3}"><Contents><Key>a</Key></Contents></ListBucketResult>'
        )

        assert svc.check_bucket_has_contents("tok", BUCKET) is True
        call = http.get.call_args
        assert call.args[0] == "https://s3.direct.eu-de.example/bucket-a?max-keys=1"
        assert call.kwargs["headers"]["Authorization"] == "Bearer tok"
        assert call.kwargs["headers"]["Resource-Crn"] == "crn:cos"
        assert call.kwargs["timeout"] == svc.S3_TIMEOUT_SECONDS

    def test_empty_bucket(self, http):
        http.get.return_value = xml_response(f'<ListBucketResult xmlns="{S3}"></ListBucketResult>')

        assert svc.check_bucket_has_contents("tok", BUCKET) is False

    def test_http_error_is_raised_instead_of_meaning_empty(self, http):
        http.get.return_value = xml_response("<Error/>", status_code=403)

        with pytest.raises(RuntimeError, match="HTTP 403"):
            svc.check_bucket_has_contents("tok", BUCKET)


class TestVersioningAndObjectLock:
    def test_versioning_enabled(self, http):
        http.get.return_value = xml_response(f'<VersioningConfiguration xmlns="{S3}"><Status>Enabled</Status></VersioningConfiguration>')

        assert svc.is_versioning_enabled("tok", "https://vpe/b") is True
        assert http.get.call_args.args[0] == "https://vpe/b?versioning"

    def test_versioning_never_configured(self, http):
        http.get.return_value = xml_response(f'<VersioningConfiguration xmlns="{S3}"/>')

        assert svc.is_versioning_enabled("tok", "https://vpe/b") is False

    def test_object_lock_enabled(self, http):
        http.get.return_value = xml_response(
            f'<ObjectLockConfiguration xmlns="{S3}"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>'
        )

        assert svc.is_object_lock_enabled("tok", "https://vpe/b") is True

    def test_object_lock_404_means_disabled(self, http):
        http.get.return_value = xml_response("<Error/>", status_code=404)

        assert svc.is_object_lock_enabled("tok", "https://vpe/b") is False


class TestLifecycle:
    def test_create_expiration_rule_sends_the_md5_of_the_body(self, http):
        http.put.return_value = xml_response("", status_code=200)

        assert svc.create_expiration_rule("tok", BUCKET) is True
        call = http.put.call_args
        assert call.args[0] == "https://s3.direct.eu-de.example/bucket-a?lifecycle"
        assert call.kwargs["data"] == svc.CLEAN_BUCKET_LIFECYCLE_CONFIGURATION.encode("utf-8")
        assert call.kwargs["headers"]["Content-MD5"]
        assert call.kwargs["headers"]["Accept"] == "application/*"
        assert b"<ID>clean_bucket</ID>" in call.kwargs["data"]

    def test_create_expiration_rule_raises_on_error(self, http):
        http.put.return_value = xml_response("", status_code=500)

        with pytest.raises(RuntimeError):
            svc.create_expiration_rule("tok", BUCKET)

    def test_delete_lifecycle_policy_returns_the_status(self, http):
        http.delete.return_value = xml_response("", status_code=204)

        assert svc.delete_lifecycle_policy("tok", BUCKET) == 204


# --- écriture -----------------------------------------------------------------------

def fresh_immutability() -> dict:
    return {
        "immutability_choice": "none",
        "object_locking_enabled": False,
        "object_versioning_enabled": True,
        "object_lock_duration_days": None,
        "object_lock_duration_years": None,
        "retention": {"retention_enabled": False, "default": None, "minimum": None, "maximum": None},
        "backup": {"backup_enabled": True, "backup_vault_sub_id": "bv-sub", "backup_retention_days": 7},
    }


class TestProcessBucketCreation:
    def test_inserts_bucket_with_workspace_and_relations(self, session):
        payload = SimpleNamespace(
            subscription_id="sub-1", requestor="karim", region="eu-de", storage_class="standard",
            environment="dev", enable_custom_permissions=False,
        )
        cos_row, vault_row = object(), object()

        result = svc.process_bucket_creation(
            payload, {"name": "realm-a"}, fresh_immutability(), {"cloudlogs": "crn:logs", "encryption_key": "crn:kms"},
            "my bucket", cos_row, vault_row, session,
        )

        bucket = session.add.call_args.args[0]
        assert isinstance(bucket, Bucket)
        assert bucket.subscription_id == "sub-1"
        assert bucket.status == SubscriptionStatus.CREATING.value
        assert bucket.created_by == "karim"
        assert bucket.kms_crn == "crn:kms"
        assert bucket.retention_enabled is False
        assert bucket.backup_enabled is True
        assert bucket.backup_retention_days == 7
        assert bucket.object_versioning_enabled is True
        assert bucket.has_expiration_rule is False
        assert bucket.cos is cos_row
        assert bucket.backup_vault is vault_row
        assert isinstance(bucket.workspace, Workspace)
        assert bucket.workspace.action == Action.APPLY.value
        assert bucket.workspace.version_type == "terraform_v1.12"
        assert bucket.workspace.status == Status.INPROGRESS.value
        session.commit.assert_called_once()
        assert result["subscription_id"] == "sub-1"


class TestUpdates:
    def test_update_bucket_status(self, session):
        svc.update_bucket_status("sub-1", SubscriptionStatus.LOCKED, session)

        (statement,) = executed(session)
        assert statement.table is Bucket
        assert statement.values_ == {"status": SubscriptionStatus.LOCKED.value}
        assert statement.where_ == ("==", "subscription_id", "sub-1")
        session.commit.assert_called_once()

    def test_update_bucket_workspace_status(self, session):
        svc.update_bucket_workspace_status("sub-1", Status.FAILED, session)

        (statement,) = executed(session)
        assert statement.table is Workspace
        assert statement.values_ == {"status": Status.FAILED.value}
        assert statement.where_ == ("==", "bucket_subscription_id", "sub-1")

    def test_update_bucket_workspace_details(self, session):
        svc.update_bucket_workspace_details({"subscription_id": "sub-1"}, {"id": "ws-1", "name": "ws_bucket_sub-1"}, session)

        (statement,) = executed(session)
        assert statement.values_ == {"name": "ws_bucket_sub-1", "workspace_id": "ws-1", "status": Status.INPROGRESS.value}

    def test_complete_bucket_create_updates_bucket_and_workspace(self, session):
        outputs = {"bucket_name": {"value": "bucket-a"}, "bucket_crn": {"value": "crn:bucket"}}

        svc.complete_bucket_create("sub-1", "https://vip/bucket-a", outputs, session)

        bucket_update, workspace_update = executed(session)
        assert bucket_update.table is Bucket
        assert bucket_update.values_ == {
            "name": "bucket-a",
            "status": SubscriptionStatus.ACTIVE.value,
            "action": Action.APPLY.value,
            "bucket_crn": "crn:bucket",
            "virtual_server_endpoint": "https://vip/bucket-a",
        }
        assert workspace_update.table is Workspace
        assert workspace_update.values_ == {"status": Status.SUCCESS.value}
        session.commit.assert_called_once()

    def test_process_bucket_update_resolves_the_vault_row(self, session, monkeypatch):
        import sys

        vault_service = MagicMock()
        vault_service.get_backup_vault_by_sub_id.return_value = SimpleNamespace(subscription_id="bv-sub")
        monkeypatch.setitem(sys.modules, "cos_service.services.backup_vault_service", vault_service)

        svc.process_bucket_update("sub-1", fresh_immutability(), True, "desc", session)

        (statement,) = executed(session)
        assert statement.values_["backup_vault_subscription_id"] == "bv-sub"
        assert statement.values_["backup_enabled"] is True
        assert statement.values_["description"] == "desc"

    def test_update_bucket_action(self, session):
        svc.update_bucket_action("sub-1", Action.DESTROY, session)

        (statement,) = executed(session)
        assert statement.table is Bucket
        assert statement.values_ == {"action": Action.DESTROY.value}

    def test_update_bucket_on_destroy(self, session):
        svc.update_bucket_on_destroy("sub-1", session)

        (statement,) = executed(session)
        assert statement.values_ == {"action": Action.DESTROY.value, "status": Status.INPROGRESS.value}
