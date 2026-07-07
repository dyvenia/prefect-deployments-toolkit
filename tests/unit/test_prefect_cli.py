"""Unit tests for prefect_deployments_toolkit.prefect_cli."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

MOD = "prefect_deployments_toolkit.prefect_cli"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> MagicMock:
    m = MagicMock(spec=subprocess.CompletedProcess)
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------


class TestRun:
    def test_returns_completed_process_on_success(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(0)):
            result = pc._run(["prefect", "version"])
        assert result.returncode == 0

    def test_raises_on_nonzero_returncode_when_check_true(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(1)):
            with pytest.raises(subprocess.CalledProcessError):
                pc._run(["prefect", "bad-command"], check=True)

    def test_does_not_raise_on_nonzero_when_check_false(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(1)):
            result = pc._run(["prefect", "bad-command"], check=False)
        assert result.returncode == 1

    def test_always_runs_with_capture_output_and_text(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc._run(["prefect", "version"])
        _, kwargs = mock_run.call_args
        assert kwargs.get("capture_output") is True
        assert kwargs.get("text") is True
        assert kwargs.get("check") is False  # _run handles check itself

    def test_stdout_lines_logged_as_info(self, caplog):
        import logging
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(
            pc.subprocess, "run", return_value=_completed(stdout="line1\nline2")
        ):
            with caplog.at_level(
                logging.INFO, logger="prefect_deployments_toolkit.prefect_cli"
            ):
                pc._run(["prefect", "version"])
        assert "line1" in caplog.text
        assert "line2" in caplog.text

    def test_stderr_lines_logged_as_info(self, caplog):
        import logging
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(
            pc.subprocess, "run", return_value=_completed(stderr="warn msg")
        ):
            with caplog.at_level(
                logging.INFO, logger="prefect_deployments_toolkit.prefect_cli"
            ):
                pc._run(["prefect", "version"])
        assert "warn msg" in caplog.text


# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------


class TestDeploy:
    def test_base_command_structure(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.deploy("my-flow", [], {}, Path("/tmp/prefect.yaml"))
        cmd = mock_run.call_args[0][0]
        assert cmd[:4] == ["prefect", "--no-prompt", "deploy", "-n"]
        assert "my-flow" in cmd

    def test_tags_appended_correctly(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.deploy("my-flow", ["v1.0", "prod"], {}, Path("/tmp/prefect.yaml"))
        cmd = mock_run.call_args[0][0]
        assert "--tag" in cmd
        tag_pairs = [cmd[i + 1] for i, x in enumerate(cmd) if x == "--tag"]
        assert set(tag_pairs) == {"v1.0", "prod"}

    def test_job_variables_appended_correctly(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.deploy(
                "my-flow",
                [],
                {"image": "myrepo/img:latest", "cpu": "2"},
                Path("/tmp/prefect.yaml"),
            )
        cmd = mock_run.call_args[0][0]
        jv_values = [cmd[i + 1] for i, x in enumerate(cmd) if x == "--job-variable"]
        assert "image=myrepo/img:latest" in jv_values
        assert "cpu=2" in jv_values

    def test_prefect_file_appended(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        pf = Path("/some/path/prefect.yaml")
        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.deploy("my-flow", [], {}, pf)
        cmd = mock_run.call_args[0][0]
        assert "--prefect-file" in cmd
        assert str(pf) in cmd

    def test_empty_tags_and_vars_produces_minimal_command(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.deploy("my-flow", [], {}, Path("/tmp/prefect.yaml"))
        cmd = mock_run.call_args[0][0]
        assert "--tag" not in cmd
        assert "--job-variable" not in cmd

    def test_raises_on_cli_failure(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(returncode=1)):
            with pytest.raises(subprocess.CalledProcessError):
                pc.deploy("my-flow", [], {}, Path("/tmp/prefect.yaml"))


# ---------------------------------------------------------------------------
# delete_deployment
# ---------------------------------------------------------------------------


class TestDeleteDeployment:
    def test_correct_command_sent(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.delete_deployment("my-flow/my-deployment")
        cmd = mock_run.call_args[0][0]
        assert cmd == [
            "prefect",
            "--no-prompt",
            "deployment",
            "delete",
            "my-flow/my-deployment",
        ]

    def test_raises_on_cli_failure(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(returncode=1)):
            with pytest.raises(subprocess.CalledProcessError):
                pc.delete_deployment("my-flow/my-deployment")


# ---------------------------------------------------------------------------
# resume_schedule
# ---------------------------------------------------------------------------


class TestResumeSchedule:
    def test_correct_command_sent(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.resume_schedule("my-flow/my-dep", "sched-uuid-123")
        cmd = mock_run.call_args[0][0]
        assert cmd == [
            "prefect",
            "deployment",
            "schedule",
            "resume",
            "my-flow/my-dep",
            "sched-uuid-123",
        ]

    def test_raises_on_cli_failure(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(returncode=1)):
            with pytest.raises(subprocess.CalledProcessError):
                pc.resume_schedule("my-flow/my-dep", "sched-uuid-123")


# ---------------------------------------------------------------------------
# delete_schedule
# ---------------------------------------------------------------------------


class TestDeleteSchedule:
    def test_correct_command_sent(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed()) as mock_run:
            pc.delete_schedule("my-flow/my-dep", "sched-uuid-456")
        cmd = mock_run.call_args[0][0]
        assert cmd == [
            "prefect",
            "deployment",
            "schedule",
            "delete",
            "-y",
            "my-flow/my-dep",
            "sched-uuid-456",
        ]

    def test_raises_on_cli_failure(self):
        from prefect_deployments_toolkit import prefect_cli as pc

        with patch.object(pc.subprocess, "run", return_value=_completed(returncode=1)):
            with pytest.raises(subprocess.CalledProcessError):
                pc.delete_schedule("my-flow/my-dep", "sched-uuid-456")
