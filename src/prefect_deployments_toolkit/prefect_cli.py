"""Thin wrappers around the Prefect CLI for deploy/delete/schedule operations."""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info("[prefect] %s", line)
    if result.stderr:
        for line in result.stderr.splitlines():
            logger.info("[prefect] %s", line)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)
    return result


def deploy(
    deployment_name: str,
    tags: list[str],
    job_variables: dict[str, str],
    prefect_file: Path,
) -> None:
    """Run `prefect deploy` for a single deployment."""
    cmd = ["prefect", "--no-prompt", "deploy", "-n", deployment_name]
    for tag in tags:
        cmd += ["--tag", tag]
    for key, value in job_variables.items():
        cmd += ["--job-variable", f"{key}={value}"]
    cmd += ["--prefect-file", str(prefect_file)]
    _run(cmd)


def delete_deployment(full_name: str) -> None:
    """Delete a deployment by its full 'flow_name/deployment_name' identifier."""
    logger.info("Deleting deployment '%s' from Prefect Cloud...", full_name)
    _run(["prefect", "--no-prompt", "deployment", "delete", full_name])


def resume_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Resume a schedule by ID."""
    logger.info("Resuming schedule %s for '%s'...", schedule_id, full_deployment_name)
    _run(
        [
            "prefect",
            "deployment",
            "schedule",
            "resume",
            full_deployment_name,
            schedule_id,
        ]
    )


def delete_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Delete a schedule by ID."""
    logger.info("Deleting schedule %s from '%s'...", schedule_id, full_deployment_name)
    _run(
        [
            "prefect",
            "deployment",
            "schedule",
            "delete",
            "-y",
            full_deployment_name,
            schedule_id,
        ]
    )
