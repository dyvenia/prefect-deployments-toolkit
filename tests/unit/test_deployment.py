"""Unit tests for prefect_deployments_toolkit.deployment."""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

from prefect_deployments_toolkit.deployment import DeploymentContext

import pytest

MOD = "prefect_deployments_toolkit.deployment"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_ctx(**overrides) -> "DeploymentContext":
    defaults = dict(
        deployment_names=["my-flow"],
        enable_schedule=False,
        tag="v1.0",
        reference="main",
        repo_name="edp-flows",
        custom_image="",
        deployments_dir=Path("deployments"),
        dev_prefect_work_pool_name="",
        backend="cli",
        enforce_unique_deployment_names=False,
    )
    defaults.update(overrides)
    return DeploymentContext(**defaults)


# ---------------------------------------------------------------------------
# DeploymentContext properties
# ---------------------------------------------------------------------------


class TestDeploymentContext:
    def test_is_dev_true_when_tag_dev_and_work_pool_set(self):
        ctx = _make_ctx(tag="dev", dev_prefect_work_pool_name="dev-pool")
        assert ctx.is_dev is True

    def test_is_dev_false_when_tag_not_dev(self):
        ctx = _make_ctx(tag="v1.0", dev_prefect_work_pool_name="dev-pool")
        assert ctx.is_dev is False

    def test_is_dev_false_when_work_pool_empty(self):
        ctx = _make_ctx(tag="dev", dev_prefect_work_pool_name="")
        assert ctx.is_dev is False

    def test_is_non_default_branch_true_for_feature_branch(self):
        ctx = _make_ctx(reference="feature/my-feature")
        assert ctx.is_non_default_branch is True

    def test_is_non_default_branch_false_for_main(self):
        assert _make_ctx(reference="main").is_non_default_branch is False

    def test_is_non_default_branch_false_for_master(self):
        assert _make_ctx(reference="master").is_non_default_branch is False

    def test_client_returns_prefect_cli_module_by_default(self):
        from prefect_deployments_toolkit import prefect_cli

        ctx = _make_ctx(backend="cli")
        assert ctx.client is prefect_cli

    def test_client_returns_prefect_rest_module_when_rest(self):
        from prefect_deployments_toolkit import prefect_rest

        ctx = _make_ctx(backend="rest")
        assert ctx.client is prefect_rest


# ---------------------------------------------------------------------------
# _resolve_flow_name
# ---------------------------------------------------------------------------


class TestResolveFlowName:
    def test_returns_empty_string_and_none_when_no_flows(self):
        from prefect_deployments_toolkit.deployment import _resolve_flow_name

        ctx = _make_ctx()
        with patch(f"{MOD}.prefect_rest.get_flow_ids_for_deployment", return_value=[]):
            flow_id, flow_name = _resolve_flow_name("my-flow", ctx)
        assert flow_id == ""
        assert flow_name is None

    def test_returns_flow_id_and_name_for_single_match(self):
        from prefect_deployments_toolkit.deployment import _resolve_flow_name

        ctx = _make_ctx()
        with (
            patch(
                f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                return_value=["fid-1"],
            ),
            patch(f"{MOD}.prefect_rest.get_flow_name", return_value="my-flow-name"),
        ):
            flow_id, flow_name = _resolve_flow_name("my-flow", ctx)
        assert flow_id == "fid-1"
        assert flow_name == "my-flow-name"

    def test_returns_first_flow_id_on_duplicates_without_enforcement(self):
        from prefect_deployments_toolkit.deployment import _resolve_flow_name

        ctx = _make_ctx(enforce_unique_deployment_names=False)
        with (
            patch(
                f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                return_value=["fid-1", "fid-2"],
            ),
            patch(
                f"{MOD}.prefect_rest.get_flow_name", side_effect=["flow-a", "flow-b"]
            ),
        ):
            flow_id, flow_name = _resolve_flow_name("my-flow", ctx)
        assert flow_id == "fid-1"
        assert flow_name == "flow-a"

    def test_returns_first_flow_id_on_duplicates_with_enforcement(self):
        from prefect_deployments_toolkit.deployment import _resolve_flow_name

        ctx = _make_ctx(enforce_unique_deployment_names=True)
        with (
            patch(
                f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                return_value=["fid-1", "fid-2"],
            ),
            patch(
                f"{MOD}.prefect_rest.get_flow_name", side_effect=["flow-a", "flow-b"]
            ),
        ):
            flow_id, flow_name = _resolve_flow_name("my-flow", ctx)
        assert flow_id == "fid-1"
        assert flow_name == "flow-a"


# ---------------------------------------------------------------------------
# _cleanup_duplicate_deployments
# ---------------------------------------------------------------------------


class TestCleanupDuplicateDeployments:
    def test_deletes_stale_flows_only(self):
        from prefect_deployments_toolkit.deployment import (
            _cleanup_duplicate_deployments,
        )

        mock_client = MagicMock()
        ctx_with_client = _make_ctx(enforce_unique_deployment_names=True)

        with (
            patch(
                f"{MOD}.prefect_rest.get_flow_name",
                side_effect=["stale-flow", "another-stale"],
            ),
            patch.object(
                type(ctx_with_client),
                "client",
                new_callable=PropertyMock,
                return_value=mock_client,
            ),
        ):
            _cleanup_duplicate_deployments(
                ctx_with_client,
                deployment_name="my-dep",
                current_flow_id="fid-current",
                current_flow_name="current-flow",
                flow_ids=["fid-current", "fid-stale", "fid-another"],
            )

        assert mock_client.delete_deployment.call_count == 2
        calls = [c[0][0] for c in mock_client.delete_deployment.call_args_list]
        assert "stale-flow/my-dep" in calls
        assert "another-stale/my-dep" in calls

    def test_does_not_delete_current_flow(self):
        from prefect_deployments_toolkit.deployment import (
            _cleanup_duplicate_deployments,
        )

        mock_client = MagicMock()
        ctx = _make_ctx()
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            _cleanup_duplicate_deployments(
                ctx,
                deployment_name="my-dep",
                current_flow_id="fid-current",
                current_flow_name="current-flow",
                flow_ids=["fid-current"],  # only current — no stale
            )

        mock_client.delete_deployment.assert_not_called()


# ---------------------------------------------------------------------------
# _build_tags
# ---------------------------------------------------------------------------


class TestBuildTags:
    def test_includes_tag_and_reference(self):
        from prefect_deployments_toolkit.deployment import _build_tags

        ctx = _make_ctx(tag="v2.0", reference="main")
        with patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]):
            tags = _build_tags(ctx, Path("/tmp/merged.yaml"), "my-flow")
        assert "v2.0" in tags
        assert "main" in tags

    def test_appends_yaml_tags(self):
        from prefect_deployments_toolkit.deployment import _build_tags

        ctx = _make_ctx(tag="v1.0", reference="main")
        with patch(
            f"{MOD}.yaml_utils.get_deployment_tags", return_value=["etl", "prod"]
        ):
            tags = _build_tags(ctx, Path("/tmp/merged.yaml"), "my-flow")
        assert "etl" in tags
        assert "prod" in tags

    def test_tag_and_reference_come_first(self):
        from prefect_deployments_toolkit.deployment import _build_tags

        ctx = _make_ctx(tag="v1.0", reference="main")
        with patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=["extra"]):
            tags = _build_tags(ctx, Path("/tmp/merged.yaml"), "my-flow")
        assert tags[0] == "v1.0"
        assert tags[1] == "main"


# ---------------------------------------------------------------------------
# _build_job_variables
# ---------------------------------------------------------------------------


class TestBuildJobVariables:
    def test_always_includes_name(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(reference="main")
        with patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["name"] == "my-flow"

    def test_custom_image_added_when_set(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(custom_image="myrepo/img:tag", reference="main")
        with patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["image"] == "myrepo/img:tag"

    def test_dbt_vars_added_on_non_default_branch(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(reference="feature-x", repo_name="edp-flows")
        with patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert "DBT_PROJECT_DIR" in jv
        assert "DBT_PROFILES_DIR" in jv
        assert "METRICS_EXPORTER_DIR" in jv
        assert "feature-x" in jv["DBT_PROJECT_DIR"]

    def test_dbt_vars_not_added_on_main(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(reference="main")
        with patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert "DBT_PROJECT_DIR" not in jv

    def test_yaml_image_does_not_override_custom_image(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(custom_image="custom:latest", reference="main")
        with patch(
            f"{MOD}.yaml_utils.get_job_variables",
            return_value={"image": "yaml-image:v1"},
        ):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["image"] == "custom:latest"

    def test_yaml_image_used_when_no_custom_image(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(custom_image="", reference="main")
        with patch(
            f"{MOD}.yaml_utils.get_job_variables",
            return_value={"image": "yaml-image:v1"},
        ):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["image"] == "yaml-image:v1"

    def test_yaml_vars_merged_into_result(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(reference="main")
        with patch(
            f"{MOD}.yaml_utils.get_job_variables",
            return_value={"cpu": "4", "memory": "8Gi"},
        ):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["cpu"] == "4"
        assert jv["memory"] == "8Gi"

    def test_yaml_var_values_coerced_to_str(self):
        from prefect_deployments_toolkit.deployment import _build_job_variables

        ctx = _make_ctx(reference="main")
        with patch(f"{MOD}.yaml_utils.get_job_variables", return_value={"replicas": 3}):
            jv = _build_job_variables(ctx, Path("/tmp/m.yaml"), "my-flow")
        assert jv["replicas"] == "3"


# ---------------------------------------------------------------------------
# _handle_schedules
# ---------------------------------------------------------------------------


class TestHandleSchedules:
    def test_deletes_existing_schedules_when_no_yaml_schedules(self):
        from prefect_deployments_toolkit.deployment import _handle_schedules

        mock_client = MagicMock()
        ctx = _make_ctx(enable_schedule=True)
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(
                    f"{MOD}.prefect_rest.get_schedule_ids", return_value=["s1", "s2"]
                ),
            ):
                _handle_schedules(ctx, Path("/tmp/m.yaml"), "my-flow", "my-flow-name")

        assert mock_client.delete_schedule.call_count == 2

    def test_no_delete_when_no_existing_schedules_and_no_yaml_schedules(self):
        from prefect_deployments_toolkit.deployment import _handle_schedules

        mock_client = MagicMock()
        ctx = _make_ctx()
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
            ):
                _handle_schedules(ctx, Path("/tmp/m.yaml"), "my-flow", "my-flow-name")

        mock_client.delete_schedule.assert_not_called()

    def test_resumes_schedules_when_enable_schedule_true(self):
        from prefect_deployments_toolkit.deployment import _handle_schedules

        mock_client = MagicMock()
        ctx = _make_ctx(enable_schedule=True)
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=True),
                patch(
                    f"{MOD}.prefect_rest.get_schedule_ids", return_value=["s1", "s2"]
                ),
            ):
                _handle_schedules(ctx, Path("/tmp/m.yaml"), "my-flow", "my-flow-name")

        assert mock_client.resume_schedule.call_count == 2

    def test_does_not_resume_when_enable_schedule_false(self):
        from prefect_deployments_toolkit.deployment import _handle_schedules

        mock_client = MagicMock()
        ctx = _make_ctx(enable_schedule=False)
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=True),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=["s1"]),
            ):
                _handle_schedules(ctx, Path("/tmp/m.yaml"), "my-flow", "my-flow-name")

        mock_client.resume_schedule.assert_not_called()


# ---------------------------------------------------------------------------
# remove_deployment
# ---------------------------------------------------------------------------


class TestRemoveDeployment:
    def test_deletes_deployment_when_flow_name_known(self):
        from prefect_deployments_toolkit.deployment import remove_deployment

        mock_client = MagicMock()
        ctx = _make_ctx()
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            remove_deployment(ctx, "my-flow", "my-flow", "my-flow-name")
        mock_client.delete_deployment.assert_called_once_with("my-flow-name/my-flow")

    def test_skips_delete_when_flow_name_is_none(self):
        from prefect_deployments_toolkit.deployment import remove_deployment

        mock_client = MagicMock()
        ctx = _make_ctx()
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            remove_deployment(ctx, "my-flow", "my-flow", None)
        mock_client.delete_deployment.assert_not_called()


# ---------------------------------------------------------------------------
# apply_single_deployment — integration through mocked collaborators
# ---------------------------------------------------------------------------


class TestApplySingleDeployment:
    """
    All external collaborators (yaml_utils, prefect_rest, ctx.client) are
    mocked. Tests verify orchestration logic, not individual helpers.

    NOTE: `ctx.client.deploy(...)` now returns a (flow_id, flow_name) tuple
    (matching the real prefect_rest.deploy / prefect_cli.deploy contract).
    Every mock_client built here has `.deploy.return_value` set explicitly —
    otherwise MagicMock's auto-generated return value fails to unpack.
    """

    def _default_patches(
        self,
        yaml_file=Path("/tmp/flow.yaml"),
        flow_id="fid-1",
        flow_name="my-flow-name",
    ):
        """Return a dict of patch targets → return values for the happy path."""
        return {
            f"{MOD}.yaml_utils.find_deployment_file": yaml_file,
            f"{MOD}._resolve_flow_name": (flow_id, flow_name),
            f"{MOD}.yaml_utils.build_merged_prefect_file": None,
            f"{MOD}.yaml_utils.validate_schedule": None,
            f"{MOD}.yaml_utils.has_schedules": False,
            f"{MOD}.yaml_utils.get_deployment_tags": [],
            f"{MOD}.yaml_utils.get_job_variables": {},
            f"{MOD}.prefect_rest.get_flow_ids_for_deployment": [flow_id],
            f"{MOD}.prefect_rest.get_flow_name": flow_name,
            f"{MOD}.prefect_rest.get_schedule_ids": [],
            f"{MOD}.time.sleep": None,
        }

    def _apply(self, ctx, patches: dict, deploy_return=("fid-1", "my-flow-name")):
        """Apply all patches and call apply_single_deployment.

        `deploy_return` is what `ctx.client.deploy(...)` will return.
        """
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        mock_client = MagicMock()
        mock_client.deploy.return_value = deploy_return
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            managers = [
                patch(target, return_value=rv) for target, rv in patches.items()
            ]
            with __import__("contextlib").ExitStack() as stack:
                mocks = {t: stack.enter_context(m) for t, m in zip(patches, managers)}
                apply_single_deployment("my-flow", ctx)
        return mock_client, mocks

    def test_calls_deploy_on_happy_path(self):
        ctx = _make_ctx(reference="main")
        patches = self._default_patches()
        mock_client, _ = self._apply(ctx, patches)
        mock_client.deploy.assert_called_once()

    def test_removes_deployment_when_yaml_file_not_found(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx()
        mock_client = MagicMock()
        mock_client.deploy.return_value = ("fid-1", "my-flow-name")
        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(f"{MOD}.yaml_utils.find_deployment_file", return_value=None),
                patch(
                    f"{MOD}._resolve_flow_name", return_value=("fid-1", "my-flow-name")
                ),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)
        mock_client.delete_deployment.assert_called_once_with("my-flow-name/my-flow")

    def test_dev_prefix_applied_when_is_dev(self):
        from prefect_deployments_toolkit.deployment import (
            apply_single_deployment,
            DEV_PREFIX,
        )

        ctx = _make_ctx(
            tag="dev", dev_prefect_work_pool_name="dev-pool", reference="main"
        )
        mock_client = MagicMock()
        deployed_names = []

        def capture_deploy(name, *a, **kw):
            deployed_names.append(name)
            return ("fid-1", "my-flow-name")

        mock_client.deploy.side_effect = capture_deploy

        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(
                    f"{MOD}.yaml_utils.find_deployment_file",
                    return_value=Path("/tmp/f.yaml"),
                ),
                patch(
                    f"{MOD}._resolve_flow_name", return_value=("fid-1", "my-flow-name")
                ),
                patch(f"{MOD}.yaml_utils.build_merged_prefect_file"),
                patch(f"{MOD}.yaml_utils.apply_dev_overrides"),
                patch(f"{MOD}.yaml_utils.validate_schedule"),
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]),
                patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}),
                patch(
                    f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                    return_value=["fid-1"],
                ),
                patch(f"{MOD}.prefect_rest.get_flow_name", return_value="my-flow-name"),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)

        assert deployed_names[0] == f"{DEV_PREFIX}my-flow"

    def test_set_git_clone_branch_called_on_non_default_branch(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx(reference="feature-x")
        mock_client = MagicMock()
        mock_client.deploy.return_value = ("fid-1", "my-flow-name")

        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(
                    f"{MOD}.yaml_utils.find_deployment_file",
                    return_value=Path("/tmp/f.yaml"),
                ),
                patch(f"{MOD}._resolve_flow_name", return_value=("", None)),
                patch(f"{MOD}.yaml_utils.build_merged_prefect_file"),
                patch(f"{MOD}.yaml_utils.validate_schedule"),
                patch(f"{MOD}.yaml_utils.set_git_clone_branch") as mock_branch,
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]),
                patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}),
                patch(
                    f"{MOD}.prefect_rest.get_flow_ids_for_deployment", return_value=[]
                ),
                patch(f"{MOD}.prefect_rest.get_flow_name", return_value=None),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)

        mock_branch.assert_called_once()
        assert mock_branch.call_args[0][1] == "feature-x"

    def test_set_git_clone_branch_not_called_on_main(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx(reference="main")
        mock_client = MagicMock()
        mock_client.deploy.return_value = ("fid-1", "my-flow-name")

        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(
                    f"{MOD}.yaml_utils.find_deployment_file",
                    return_value=Path("/tmp/f.yaml"),
                ),
                patch(
                    f"{MOD}._resolve_flow_name", return_value=("fid-1", "my-flow-name")
                ),
                patch(f"{MOD}.yaml_utils.build_merged_prefect_file"),
                patch(f"{MOD}.yaml_utils.validate_schedule"),
                patch(f"{MOD}.yaml_utils.set_git_clone_branch") as mock_branch,
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]),
                patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}),
                patch(
                    f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                    return_value=["fid-1"],
                ),
                patch(f"{MOD}.prefect_rest.get_flow_name", return_value="my-flow-name"),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)

        mock_branch.assert_not_called()

    def test_stale_deployment_deleted_on_flow_rename(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx(reference="main")
        mock_client = MagicMock()
        # Deploy resolves to a NEW flow_id/flow_name, different from the
        # pre-deploy ("fid-old", "old-flow-name") resolved by _resolve_flow_name.
        mock_client.deploy.return_value = ("fid-new", "new-flow-name")

        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(
                    f"{MOD}.yaml_utils.find_deployment_file",
                    return_value=Path("/tmp/f.yaml"),
                ),
                patch(
                    f"{MOD}._resolve_flow_name",
                    return_value=("fid-old", "old-flow-name"),
                ),
                patch(f"{MOD}.yaml_utils.build_merged_prefect_file"),
                patch(f"{MOD}.yaml_utils.validate_schedule"),
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=False),
                patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]),
                patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}),
                # Post-deploy: flow_id changed
                patch(
                    f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                    return_value=["fid-new"],
                ),
                patch(
                    f"{MOD}.prefect_rest.get_flow_name", return_value="new-flow-name"
                ),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)

        mock_client.delete_deployment.assert_called_once_with("old-flow-name/my-flow")

    def test_merged_file_cleaned_up_on_exception(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx(reference="main")
        unlinked = []

        with (
            patch(
                f"{MOD}.yaml_utils.find_deployment_file",
                return_value=Path("/tmp/f.yaml"),
            ),
            patch(f"{MOD}._resolve_flow_name", return_value=("fid-1", "flow-name")),
            patch(
                f"{MOD}.yaml_utils.build_merged_prefect_file",
                side_effect=RuntimeError("boom"),
            ),
            patch(f"{MOD}.time.sleep"),
            patch(
                "pathlib.Path.unlink",
                side_effect=lambda missing_ok=False: unlinked.append(True),
            ),
        ):
            with pytest.raises(RuntimeError):
                apply_single_deployment("my-flow", ctx)

        assert unlinked, (
            "merged_file.unlink() should be called in finally even on exception"
        )

    def test_set_schedules_active_called_when_enable_schedule_and_has_schedules(self):
        from prefect_deployments_toolkit.deployment import apply_single_deployment

        ctx = _make_ctx(enable_schedule=True, reference="main")
        mock_client = MagicMock()
        mock_client.deploy.return_value = ("fid-1", "flow-name")

        with patch.object(
            type(ctx), "client", new_callable=PropertyMock, return_value=mock_client
        ):
            with (
                patch(
                    f"{MOD}.yaml_utils.find_deployment_file",
                    return_value=Path("/tmp/f.yaml"),
                ),
                patch(f"{MOD}._resolve_flow_name", return_value=("fid-1", "flow-name")),
                patch(f"{MOD}.yaml_utils.build_merged_prefect_file"),
                patch(f"{MOD}.yaml_utils.validate_schedule"),
                patch(f"{MOD}.yaml_utils.has_schedules", return_value=True),
                patch(f"{MOD}.yaml_utils.set_schedules_active") as mock_active,
                patch(f"{MOD}.yaml_utils.get_deployment_tags", return_value=[]),
                patch(f"{MOD}.yaml_utils.get_job_variables", return_value={}),
                patch(
                    f"{MOD}.prefect_rest.get_flow_ids_for_deployment",
                    return_value=["fid-1"],
                ),
                patch(f"{MOD}.prefect_rest.get_flow_name", return_value="flow-name"),
                patch(f"{MOD}.prefect_rest.get_schedule_ids", return_value=[]),
                patch(f"{MOD}.time.sleep"),
            ):
                apply_single_deployment("my-flow", ctx)

        mock_active.assert_called_once()
        assert (
            mock_active.call_args[1]["active"] is True
            or mock_active.call_args[0][2] is True
        )
