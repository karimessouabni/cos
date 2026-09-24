"""Tests du service Schematics : appels au backend avec un ``MagicMock``."""
from unittest.mock import MagicMock

import pytest

from bp2i_airflow_library.config import OrchestratorEnvironment
from cos_service.services import schematics_service as svc
from cos_service.utils import constants


@pytest.fixture
def tf():
    backend = MagicMock(name="tf")
    backend.workspaces.create_or_update.return_value = {"id": "ws-1", "name": "ws_bucket_sub-1"}
    return backend


class TestSettingsFor:
    def test_accepts_the_enum_value(self):
        settings = svc.settings_for(OrchestratorEnvironment.PROD.value)
        assert settings.branch == "prod"
        assert "env:prod" in settings.tags

    def test_accepts_the_enum_itself(self):
        assert svc.settings_for(OrchestratorEnvironment.PREPROD).branch == "preprod"

    def test_int_branch_is_the_configurable_one(self):
        assert svc.settings_for(OrchestratorEnvironment.INT).branch == svc.INT_BRANCH

    def test_unknown_environment_is_an_explicit_error(self):
        with pytest.raises(ValueError, match="Unknown orchestrator environment 'staging'"):
            svc.settings_for("staging")


class TestCreateOrUpdateWs:
    def test_builds_the_workspace_from_env_settings(self, tf):
        result = svc.create_or_update_ws(
            tf, "ws_bucket_sub-1", OrchestratorEnvironment.PROD.value,
            "terraform/v1.12/bucket", {"region": "eu-de"}, "my bucket", "gl-token",
        )

        assert result == {"id": "ws-1", "name": "ws_bucket_sub-1"}
        call = tf.workspaces.create_or_update.call_args
        assert call.args == ("ws_bucket_sub-1",)
        assert call.kwargs["tags"] == ["env:prod", "agent:ga", "version:1.0"]
        assert call.kwargs["tf_version"] == "terraform_v1.12"
        assert call.kwargs["variables"] == {"region": "eu-de"}
        assert call.kwargs["description"] == "my bucket"
        assert call.kwargs["project"] == svc.SCHEMATICS_PROJECT
        assert call.kwargs["env_values"] == [{"TF_LOG": svc.TF_LOG_LEVEL}]
        vcs = call.kwargs["vcs"]
        assert vcs.repository == constants.TERRAFORM_REPOSITORY
        assert vcs.branch == "prod"
        assert vcs.oauth_token_id == "gl-token"
        assert vcs.directory == "terraform/v1.12/bucket"

    def test_keyword_call_matches_the_dag_usage(self, tf):
        svc.create_or_update_ws(
            tf,
            workspace_name="ws",
            orchestrator_env=OrchestratorEnvironment.INT.value,
            tf_directory="terraform/v1.12/bucket",
            variables={},
            description="d",
            gitlab_token="t",
        )

        assert tf.workspaces.create_or_update.call_args.kwargs["vcs"].branch == svc.INT_BRANCH

    def test_unknown_environment_does_not_reach_schematics(self, tf):
        with pytest.raises(ValueError):
            svc.create_or_update_ws(tf, "ws", "staging", "dir", {}, "d", "t")

        tf.workspaces.create_or_update.assert_not_called()


class TestUpdateWs:
    def test_updates_everything_but_the_variables(self, tf):
        svc.update_ws(tf, "ws-1", OrchestratorEnvironment.PREPROD.value, "dir", "desc", "tok")

        call = tf.workspaces.update.call_args
        assert call.kwargs["workspace_id"] == "ws-1"
        assert call.kwargs["tags"] == ["env:pprod", "agent:ga", "version:1.0"]
        assert call.kwargs["vcs"].branch == "preprod"
        assert "variables" not in call.kwargs


class TestUpdateWsVariables:
    def test_forwards_the_variables_and_logs_only_their_names(self, tf, caplog):
        caplog.set_level("DEBUG", logger=svc.logger.name)
        variables = {"vault_read_token": "SECRET-VALUE", "region": "eu-de"}

        svc.update_ws_variables(tf, "ws-1", variables)

        tf.workspaces.update_variables.assert_called_once_with(workspace_id="ws-1", variables=variables)
        assert "vault_read_token" in caplog.text
        assert "SECRET-VALUE" not in caplog.text


class TestRunWorkspace:
    @pytest.fixture
    def workspace(self, tf):
        workspace = tf.workspaces.get_by_id.return_value
        workspace.apply.return_value.get_logs.return_value = {"tpl-1": "apply logs"}
        workspace.get_outputs.return_value = [
            MagicMock(output_values=[{"bucket_name": {"value": "bucket-a"}}])
        ]
        return workspace

    def test_plans_applies_and_returns_the_first_outputs(self, tf, workspace, caplog):
        caplog.set_level("INFO", logger=svc.logger.name)

        result = svc.run_workspace(tf, "ws-1")

        assert result == {"bucket_name": {"value": "bucket-a"}}
        tf.workspaces.get_by_id.assert_called_once_with("ws-1")
        workspace.plan.assert_called_once()
        workspace.apply.assert_called_once()
        assert "apply logs" in caplog.text

    def test_no_outputs_is_an_explicit_error(self, tf, workspace):
        workspace.get_outputs.return_value = []

        with pytest.raises(RuntimeError, match="no Terraform outputs"):
            svc.run_workspace(tf, "ws-1")

    def test_empty_output_values_is_an_explicit_error(self, tf, workspace):
        workspace.get_outputs.return_value = [MagicMock(output_values=[])]

        with pytest.raises(RuntimeError, match="no Terraform outputs"):
            svc.run_workspace(tf, "ws-1")
