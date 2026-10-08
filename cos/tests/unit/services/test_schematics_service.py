"""Tests du service Schematics : appels au backend avec un ``MagicMock``."""
import sys
import types
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


@pytest.fixture
def no_overrides(monkeypatch):
    """Ni Airflow Variable ni variable d'environnement : les défauts s'appliquent."""
    monkeypatch.delenv("COS_TF_BRANCH", raising=False)
    monkeypatch.delenv("COS_TF_LOG_LEVEL", raising=False)
    monkeypatch.delitem(sys.modules, "airflow.models", raising=False)
    monkeypatch.setattr(sys.modules["airflow"], "models", None, raising=False)


@pytest.fixture
def airflow_variables(monkeypatch):
    """Doublure de ``airflow.models.Variable`` alimentée par un dict."""
    store = {}

    class Variable:
        @staticmethod
        def get(name, default_var=None):
            return store.get(name, default_var)

    models = types.ModuleType("airflow.models")
    models.Variable = Variable
    monkeypatch.setitem(sys.modules, "airflow.models", models)
    monkeypatch.setattr(sys.modules["airflow"], "models", models, raising=False)
    return store


class TestSettingsFor:
    def test_accepts_the_enum_value(self, no_overrides):
        settings = svc.settings_for(OrchestratorEnvironment.PROD.value)
        assert settings.branch == "prod"
        assert "env:prod" in settings.tags

    def test_accepts_the_enum_itself(self, no_overrides):
        assert svc.settings_for(OrchestratorEnvironment.PREPROD).branch == "preprod"

    @pytest.mark.parametrize("env, branch, tf_log, tag", [
        ("int", "main", "DEBUG", "env:int"),
        ("preprod", "preprod", "INFO", "env:pprod"),
        ("prod", "prod", "ERROR", "env:prod"),
    ])
    def test_defaults_follow_the_ci_branches(self, no_overrides, env, branch, tf_log, tag):
        settings = svc.settings_for(env)

        assert settings.branch == branch
        assert settings.tf_log == tf_log
        assert settings.tags == [tag, "agent:ga", "version:1.0"]

    def test_no_feature_branch_is_hardcoded(self):
        assert not any("feature/" in b for b in svc.DEFAULT_BRANCHES.values())

    def test_unknown_environment_is_an_explicit_error(self, no_overrides):
        with pytest.raises(ValueError, match="Unknown orchestrator environment 'staging'"):
            svc.settings_for("staging")

    def test_environment_variables_override_the_defaults(self, no_overrides, monkeypatch):
        monkeypatch.setenv("COS_TF_BRANCH", "feature/xyz")
        monkeypatch.setenv("COS_TF_LOG_LEVEL", "TRACE")

        settings = svc.settings_for(OrchestratorEnvironment.INT)

        assert settings.branch == "feature/xyz"
        assert settings.tf_log == "TRACE"

    def test_airflow_variables_win_over_environment_variables(self, airflow_variables, monkeypatch):
        monkeypatch.setenv("COS_TF_BRANCH", "from-env")
        airflow_variables["cos_tf_branch"] = "feature/from-airflow"

        assert svc.settings_for("int").branch == "feature/from-airflow"

    def test_unset_airflow_variable_falls_back_to_env_then_default(self, airflow_variables, monkeypatch):
        monkeypatch.setenv("COS_TF_LOG_LEVEL", "WARN")
        monkeypatch.delenv("COS_TF_BRANCH", raising=False)

        settings = svc.settings_for("prod")

        assert settings.tf_log == "WARN"
        assert settings.branch == "prod"

    def test_settings_are_read_at_call_time(self, no_overrides, monkeypatch):
        assert svc.settings_for("int").branch == "main"
        monkeypatch.setenv("COS_TF_BRANCH", "feature/later")
        assert svc.settings_for("int").branch == "feature/later"

    def test_unreachable_airflow_metadata_does_not_block(self, airflow_variables, monkeypatch, caplog):
        def boom(name, default_var=None):
            raise RuntimeError("metadata db down")

        monkeypatch.setattr(sys.modules["airflow.models"].Variable, "get", staticmethod(boom))
        monkeypatch.delenv("COS_TF_BRANCH", raising=False)
        caplog.set_level("WARNING", logger=svc.logger.name)

        assert svc.settings_for("int").branch == "main"
        assert "could not read Airflow Variable cos_tf_branch" in caplog.text


class TestProductBranch:
    """La branche de la demande l'emporte : Schematics clone celle sur laquelle le DAG tourne."""

    def test_product_branch_is_used_on_int(self, no_overrides):
        assert svc.settings_for("int", product_branch="feature/retention").branch == "feature/retention"

    def test_product_branch_wins_over_the_overrides(self, airflow_variables, monkeypatch):
        airflow_variables["cos_tf_branch"] = "feature/from-variable"
        monkeypatch.setenv("COS_TF_BRANCH", "feature/from-env")

        assert svc.settings_for("int", product_branch="feature/from-demand").branch == "feature/from-demand"

    def test_without_product_branch_the_overrides_then_defaults_apply(self, no_overrides, monkeypatch):
        assert svc.settings_for("int", product_branch=None).branch == "main"
        monkeypatch.setenv("COS_TF_BRANCH", "feature/x")
        assert svc.settings_for("int", product_branch="").branch == "feature/x"

    @pytest.mark.parametrize("env, branch", [("preprod", "preprod"), ("prod", "prod")])
    def test_environment_branch_is_accepted_in_pprod_and_prod(self, no_overrides, env, branch):
        assert svc.settings_for(env, product_branch=branch).branch == branch

    @pytest.mark.parametrize("env", ["preprod", "prod"])
    def test_a_feature_branch_is_refused_in_pprod_and_prod(self, no_overrides, env):
        with pytest.raises(ValueError, match=f"Environment '{env}' only runs Terraform from branch"):
            svc.settings_for(env, product_branch="feature/oops")

    @pytest.mark.parametrize("env", ["preprod", "prod"])
    def test_overrides_cannot_redirect_pprod_and_prod_either(self, no_overrides, monkeypatch, env):
        monkeypatch.setenv("COS_TF_BRANCH", "feature/oops")

        with pytest.raises(ValueError, match="only runs Terraform from branch"):
            svc.settings_for(env)


class TestCreateOrUpdateWs:
    def test_product_branch_is_forwarded_to_the_vcs(self, tf, no_overrides):
        svc.create_or_update_ws(
            tf, "ws", OrchestratorEnvironment.INT.value, "dir", {}, "d", "t", product_branch="feature/z"
        )

        assert tf.workspaces.create_or_update.call_args.kwargs["vcs"].branch == "feature/z"

    def test_feature_branch_in_prod_does_not_reach_schematics(self, tf, no_overrides):
        with pytest.raises(ValueError):
            svc.create_or_update_ws(tf, "ws", "prod", "dir", {}, "d", "t", product_branch="feature/z")

        tf.workspaces.create_or_update.assert_not_called()

    def test_builds_the_workspace_from_env_settings(self, tf, no_overrides):
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
        assert call.kwargs["env_values"] == [{"TF_LOG": "ERROR"}]
        vcs = call.kwargs["vcs"]
        assert vcs.repository == constants.TERRAFORM_REPOSITORY
        assert vcs.branch == "prod"
        assert vcs.oauth_token_id == "gl-token"
        assert vcs.directory == "terraform/v1.12/bucket"

    def test_keyword_call_matches_the_dag_usage(self, tf, no_overrides):
        svc.create_or_update_ws(
            tf,
            workspace_name="ws",
            orchestrator_env=OrchestratorEnvironment.INT.value,
            tf_directory="terraform/v1.12/bucket",
            variables={},
            description="d",
            gitlab_token="t",
        )

        assert tf.workspaces.create_or_update.call_args.kwargs["vcs"].branch == "main"

    def test_unknown_environment_does_not_reach_schematics(self, tf):
        with pytest.raises(ValueError):
            svc.create_or_update_ws(tf, "ws", "staging", "dir", {}, "d", "t")

        tf.workspaces.create_or_update.assert_not_called()


class TestUpdateWs:
    def test_product_branch_is_forwarded(self, tf, no_overrides):
        svc.update_ws(tf, "ws-1", "int", "dir", "desc", "tok", product_branch="feature/y")

        assert tf.workspaces.update.call_args.kwargs["vcs"].branch == "feature/y"

    def test_updates_everything_but_the_variables(self, tf, no_overrides):
        svc.update_ws(tf, "ws-1", OrchestratorEnvironment.PREPROD.value, "dir", "desc", "tok")

        call = tf.workspaces.update.call_args
        assert call.kwargs["workspace_id"] == "ws-1"
        assert call.kwargs["tags"] == ["env:pprod", "agent:ga", "version:1.0"]
        assert call.kwargs["vcs"].branch == "preprod"
        assert call.kwargs["env_values"] == [{"TF_LOG": "INFO"}]
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
        workspace.plan.return_value.get_logs.return_value = {"tpl-1": "...\nPlan: 3 to add, 1 to change, 0 to destroy.\n"}

        result = svc.run_workspace(tf, "ws-1")

        assert result == {"bucket_name": {"value": "bucket-a"}}
        tf.workspaces.get_by_id.assert_called_once_with("ws-1")
        workspace.plan.assert_called_once()
        workspace.apply.assert_called_once()
        assert "plan logs of workspace ws-1" in caplog.text
        assert "plan of workspace ws-1, template tpl-1: 3 to add, 1 to change, 0 to destroy" in caplog.text
        assert "apply logs" in caplog.text
        assert "plan of workspace ws-1 done in" in caplog.text and "apply of workspace ws-1 done in" in caplog.text
        assert "outputs of workspace ws-1: ['bucket_name']" in caplog.text
        assert "bucket-a" not in caplog.text.split("outputs of workspace")[1]  # noms des outputs, pas leurs valeurs
        assert "WARNING" not in caplog.text

    def test_a_destroying_plan_is_a_warning(self, tf, workspace, caplog):
        caplog.set_level("INFO", logger=svc.logger.name)
        workspace.plan.return_value.get_logs.return_value = {"tpl-1": "Plan: 0 to add, 0 to change, 2 to destroy."}

        svc.run_workspace(tf, "ws-1")

        assert "plan of workspace ws-1 destroys 2 resource(s)" in caplog.text
        assert caplog.records[-1].levelname == "INFO"  # le warning n'empêche rien
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_no_changes_plan_is_summarised(self, tf, workspace, caplog):
        caplog.set_level("INFO", logger=svc.logger.name)
        workspace.plan.return_value.get_logs.return_value = {"tpl-1": "No changes. Your infrastructure matches the configuration."}

        svc.run_workspace(tf, "ws-1")

        assert "plan of workspace ws-1, template tpl-1: no changes" in caplog.text

    def test_plan_without_logs_api_still_runs(self, tf, workspace, caplog):
        """Ancienne bp2i_terraform : plan() ne renvoie pas d'activité avec get_logs."""
        caplog.set_level("INFO", logger=svc.logger.name)
        workspace.plan.return_value = None

        assert svc.run_workspace(tf, "ws-1") == {"bucket_name": {"value": "bucket-a"}}
        assert "plan logs" not in caplog.text

    def test_unreadable_logs_never_fail_the_run(self, tf, workspace, caplog):
        caplog.set_level("INFO", logger=svc.logger.name)
        workspace.apply.return_value.get_logs.side_effect = RuntimeError("logs api down")

        assert svc.run_workspace(tf, "ws-1") == {"bucket_name": {"value": "bucket-a"}}
        assert "could not read Schematics activity logs: logs api down" in caplog.text

    def test_no_outputs_is_an_explicit_error(self, tf, workspace):
        workspace.get_outputs.return_value = []

        with pytest.raises(RuntimeError, match="no Terraform outputs"):
            svc.run_workspace(tf, "ws-1")

    def test_empty_output_values_is_an_explicit_error(self, tf, workspace):
        workspace.get_outputs.return_value = [MagicMock(output_values=[])]

        with pytest.raises(RuntimeError, match="no Terraform outputs"):
            svc.run_workspace(tf, "ws-1")
