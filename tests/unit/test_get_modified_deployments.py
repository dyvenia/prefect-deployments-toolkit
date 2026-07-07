"""Unit tests for prefect_deployments_toolkit.get_modified_deployments."""

import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

# Shorthand module path used throughout
MOD = "prefect_deployments_toolkit.get_modified_deployments"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

DEP_A = {"name": "flow-a", "entrypoint": "flows/a.py:flow_a"}
DEP_B = {"name": "flow-b", "entrypoint": "flows/b.py:flow_b"}
DEP_A_MODIFIED = {"name": "flow-a", "entrypoint": "flows/a.py:flow_a", "tags": ["new"]}


def _mock_run_ok():
    m = MagicMock()
    m.returncode = 0
    return m


def _mock_run_fail():
    m = MagicMock()
    m.returncode = 1
    return m


# ---------------------------------------------------------------------------
# get_base_branch_deployments
# ---------------------------------------------------------------------------


class TestGetBaseBranchDeployments:
    def test_fetches_origin_then_loads_source(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                gmd.subprocess, "run", return_value=_mock_run_ok()
            ) as mock_run,
            patch.object(
                gmd, "get_deployments_from_source", return_value={"flow-a": DEP_A}
            ) as mock_load,
        ):
            result = gmd.get_base_branch_deployments("main", "deployments")

        mock_run.assert_called_once_with(
            ["git", "fetch", "origin", "main"],
            check=True,
            stderr=subprocess.DEVNULL,
        )
        mock_load.assert_called_once_with(
            "origin/main", "the base branch 'main'", "deployments"
        )
        assert result == {"flow-a": DEP_A}

    def test_fetch_failure_is_tolerated_and_load_still_called(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                gmd.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, "git fetch"),
            ),
            patch.object(
                gmd, "get_deployments_from_source", return_value={}
            ) as mock_load,
        ):
            result = gmd.get_base_branch_deployments("main")

        mock_load.assert_called_once()
        assert result == {}

    def test_uses_custom_deployments_dir(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(gmd.subprocess, "run", return_value=_mock_run_ok()),
            patch.object(
                gmd, "get_deployments_from_source", return_value={}
            ) as mock_load,
        ):
            gmd.get_base_branch_deployments("rc_3.3.1", "custom/deployments")

        mock_load.assert_called_once_with(
            "origin/rc_3.3.1", "the base branch 'rc_3.3.1'", "custom/deployments"
        )


# ---------------------------------------------------------------------------
# get_pr_branch_deployments
# ---------------------------------------------------------------------------


class TestGetPrBranchDeployments:
    def test_calls_get_deployments_from_source_with_local(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with patch.object(
            gmd, "get_deployments_from_source", return_value={"flow-a": DEP_A}
        ) as mock_load:
            result = gmd.get_pr_branch_deployments("deployments")

        mock_load.assert_called_once_with("local", "the current branch", "deployments")
        assert result == {"flow-a": DEP_A}

    def test_default_deployments_dir(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with patch.object(
            gmd, "get_deployments_from_source", return_value={}
        ) as mock_load:
            gmd.get_pr_branch_deployments()

        mock_load.assert_called_once_with("local", "the current branch", "deployments")


# ---------------------------------------------------------------------------
# get_previous_commit_deployments
# ---------------------------------------------------------------------------


class TestGetPreviousCommitDeployments:
    def test_returns_empty_when_no_head_tilde1(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(gmd.subprocess, "run", return_value=_mock_run_fail()),
            patch.object(gmd, "get_deployments_from_source") as mock_load,
        ):
            result = gmd.get_previous_commit_deployments()

        mock_load.assert_not_called()
        assert result == {}

    def test_loads_head_tilde1_when_it_exists(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(gmd.subprocess, "run", return_value=_mock_run_ok()),
            patch.object(
                gmd, "get_deployments_from_source", return_value={"flow-a": DEP_A}
            ) as mock_load,
        ):
            result = gmd.get_previous_commit_deployments("deployments")

        mock_load.assert_called_once_with(
            "HEAD~1", "the previous commit", "deployments"
        )
        assert result == {"flow-a": DEP_A}

    def test_uses_custom_deployments_dir(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(gmd.subprocess, "run", return_value=_mock_run_ok()),
            patch.object(
                gmd, "get_deployments_from_source", return_value={}
            ) as mock_load,
        ):
            gmd.get_previous_commit_deployments("custom/dir")

        mock_load.assert_called_once_with("HEAD~1", "the previous commit", "custom/dir")


# ---------------------------------------------------------------------------
# main() — argument routing logic
# ---------------------------------------------------------------------------


class TestMain:
    """
    Strategy: patch the three helper functions and sys.argv, then call main().
    Verify which helpers are called (previous/current resolution) and what
    gets printed / written to GITHUB_ENV.
    """

    def _run_main(
        self, argv: list[str], previous: dict, current: dict, env: dict | None = None
    ):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(sys, "argv", ["prog"] + argv),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value=previous
            ) as mock_prev,
            patch.object(
                gmd, "get_base_branch_deployments", return_value=current
            ) as mock_base,
            patch.object(
                gmd, "get_pr_branch_deployments", return_value=current
            ) as mock_pr,
            patch.dict(os.environ, env or {}, clear=False),
        ):
            try:
                gmd.main()
            except SystemExit:
                pass
            return mock_prev, mock_base, mock_pr

    # --- event routing ---

    def test_push_uses_previous_commit_and_base_branch(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={}
            ) as mock_prev,
            patch.object(
                gmd, "get_base_branch_deployments", return_value={}
            ) as mock_base,
            patch.object(gmd, "get_pr_branch_deployments", return_value={}) as mock_pr,
        ):
            with pytest.raises(SystemExit):
                gmd.main()

        mock_prev.assert_called_once()
        mock_base.assert_called_once()
        mock_pr.assert_not_called()

    def test_pull_request_target_uses_previous_commit_and_base_branch(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys,
                "argv",
                ["prog", "--modified-by", "pull_request_target", "--base-ref", "main"],
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={}
            ) as mock_prev,
            patch.object(
                gmd, "get_base_branch_deployments", return_value={}
            ) as mock_base,
            patch.object(gmd, "get_pr_branch_deployments", return_value={}) as mock_pr,
        ):
            with pytest.raises(SystemExit):
                gmd.main()

        mock_prev.assert_called_once()
        mock_base.assert_called_once()
        mock_pr.assert_not_called()

    def test_pull_request_uses_base_branch_and_pr_branch(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys,
                "argv",
                ["prog", "--modified-by", "pull_request", "--base-ref", "main"],
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={}
            ) as mock_prev,
            patch.object(
                gmd, "get_base_branch_deployments", return_value={}
            ) as mock_base,
            patch.object(gmd, "get_pr_branch_deployments", return_value={}) as mock_pr,
        ):
            with pytest.raises(SystemExit):
                gmd.main()

        mock_prev.assert_not_called()
        mock_base.assert_called_once()
        mock_pr.assert_called_once()

    def test_no_modified_by_uses_base_branch_and_pr_branch(self):
        """Omitting --modified-by (None) is treated as PR path (not commit compare)."""
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(sys, "argv", ["prog", "--base-ref", "main"]),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={}
            ) as mock_prev,
            patch.object(gmd, "get_base_branch_deployments", return_value={}),
            patch.object(gmd, "get_pr_branch_deployments", return_value={}) as mock_pr,
        ):
            with pytest.raises(SystemExit):
                gmd.main()

        mock_prev.assert_not_called()
        mock_pr.assert_called_once()

    # --- change detection ---

    def test_exits_0_when_no_changes(self, capsys):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        deps = {"flow-a": DEP_A}
        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(gmd, "get_previous_commit_deployments", return_value=deps),
            patch.object(gmd, "get_base_branch_deployments", return_value=deps),
        ):
            with pytest.raises(SystemExit) as exc_info:
                gmd.main()

        assert exc_info.value.code == 0

    def test_detects_new_deployment(self, capsys):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(gmd, "get_previous_commit_deployments", return_value={}),
            patch.object(
                gmd, "get_base_branch_deployments", return_value={"flow-a": DEP_A}
            ),
        ):
            gmd.main()

        output = capsys.readouterr().out.strip()
        assert "flow-a" in output

    def test_detects_modified_deployment(self, capsys):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={"flow-a": DEP_A}
            ),
            patch.object(
                gmd,
                "get_base_branch_deployments",
                return_value={"flow-a": DEP_A_MODIFIED},
            ),
        ):
            gmd.main()

        output = capsys.readouterr().out.strip()
        assert "flow-a" in output

    def test_detects_removed_deployment(self, capsys):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={"flow-a": DEP_A}
            ),
            patch.object(gmd, "get_base_branch_deployments", return_value={}),
        ):
            gmd.main()

        output = capsys.readouterr().out.strip()
        assert "flow-a" in output

    def test_output_is_comma_separated(self, capsys):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(gmd, "get_previous_commit_deployments", return_value={}),
            patch.object(
                gmd,
                "get_base_branch_deployments",
                return_value={"flow-a": DEP_A, "flow-b": DEP_B},
            ),
        ):
            gmd.main()

        output = capsys.readouterr().out.strip()
        names = output.split(",")
        assert set(names) == {"flow-a", "flow-b"}

    # --- GITHUB_ENV writing ---

    def test_writes_github_env_when_env_vars_set(self, tmp_path):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        env_file = tmp_path / "github.env"
        env_file.write_text("")

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(gmd, "get_previous_commit_deployments", return_value={}),
            patch.object(
                gmd, "get_base_branch_deployments", return_value={"flow-a": DEP_A}
            ),
            patch.dict(os.environ, {"GITHUB_ACTION": "1", "GITHUB_ENV": str(env_file)}),
        ):
            gmd.main()

        content = env_file.read_text()
        assert "DEPLOYMENT_NAMES=flow-a" in content
        assert "NEW_OR_MODIFIED_DEPLOYMENT_NAMES=flow-a" in content
        assert "REMOVED_DEPLOYMENT_NAMES=" in content

    def test_github_env_includes_removed_deployments(self, tmp_path):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        env_file = tmp_path / "github.env"
        env_file.write_text("")

        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={"flow-a": DEP_A}
            ),
            patch.object(
                gmd, "get_base_branch_deployments", return_value={"flow-b": DEP_B}
            ),
            patch.dict(os.environ, {"GITHUB_ACTION": "1", "GITHUB_ENV": str(env_file)}),
        ):
            gmd.main()

        content = env_file.read_text()
        assert "REMOVED_DEPLOYMENT_NAMES=flow-a" in content
        assert "NEW_OR_MODIFIED_DEPLOYMENT_NAMES=flow-b" in content

    def test_github_env_not_written_when_env_vars_absent(self, tmp_path):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        env_file = tmp_path / "github.env"
        env_file.write_text("")

        clean_env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("GITHUB_ACTION", "GITHUB_ENV")
        }
        with (
            patch.object(
                sys, "argv", ["prog", "--modified-by", "push", "--base-ref", "main"]
            ),
            patch.object(gmd, "get_previous_commit_deployments", return_value={}),
            patch.object(
                gmd, "get_base_branch_deployments", return_value={"flow-a": DEP_A}
            ),
            patch.dict(os.environ, clean_env, clear=True),
        ):
            gmd.main()

        assert env_file.read_text() == ""

    def test_uses_custom_deployments_dir_arg(self):
        from prefect_deployments_toolkit import get_modified_deployments as gmd

        with (
            patch.object(
                sys,
                "argv",
                [
                    "prog",
                    "--modified-by",
                    "push",
                    "--base-ref",
                    "main",
                    "--deployments-dir",
                    "custom/dir",
                ],
            ),
            patch.object(
                gmd, "get_previous_commit_deployments", return_value={}
            ) as mock_prev,
            patch.object(
                gmd, "get_base_branch_deployments", return_value={}
            ) as mock_base,
        ):
            with pytest.raises(SystemExit):
                gmd.main()

        mock_prev.assert_called_once_with("custom/dir")
        mock_base.assert_called_once_with("main", "custom/dir")
