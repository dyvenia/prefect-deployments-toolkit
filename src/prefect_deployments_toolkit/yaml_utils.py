"""YAML file helpers for reading and mutating prefect deployment configs."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

SCHEDULE_ERRORS = {
    "singular_key": "Use 'schedules' (plural), not 'schedule' (singular).",
    "empty_schedules": "'schedules' key is present but empty or null.",
    "missing_cron": "{count} schedule entry/entries missing required 'cron' field.",
}


def find_deployment_file(deployment_name: str, deployments_dir: Path) -> Path | None:
    """Find the single YAML file that declares a given deployment.

    Returns the Path if exactly one match is found.
    Returns None if no match (deployment was removed).
    Raises ValueError if multiple matches are found.
    """
    logger.info("Looking for deployment '%s' in deployment files...", deployment_name)
    pattern = rf'^\s*-\s*name:\s*["\']?{deployment_name}["\']?\s*$'
    result = subprocess.run(
        [
            "grep",
            "-r",
            "--include=*.yaml",
            "--exclude=prefect_base.yaml",
            "-P",
            pattern,
            str(deployments_dir),
        ],
        capture_output=True,
        text=True,
    )
    matches = [line for line in result.stdout.splitlines() if line.strip()]
    file_paths = list({line.split(":")[0] for line in matches})

    if not file_paths:
        logger.error(
            "\tDeployment '%s' not found in any file — will be removed from Prefect Cloud (if exists).",
            deployment_name,
        )
        return None
    if len(file_paths) > 1:
        raise ValueError(
            f"Deployment '{deployment_name}' found in multiple files: {file_paths}. "
            "Duplicate deployment names must be resolved before proceeding."
        )
    path = Path(file_paths[0])
    logger.info("\tDeployment '%s' found in: %s", deployment_name, path)
    return path


def build_merged_prefect_file(
    base_file: Path,
    deployment_file: Path,
    dest: Path,
) -> None:
    """Concatenate prefect_base.yaml and a deployment file into a temp file."""
    dest.write_text(base_file.read_text() + "\n" + deployment_file.read_text())


def load_deployment_config(merged_file: Path, deployment_name: str) -> dict:
    """Return the parsed config dict for a named deployment from a merged YAML file."""
    content = yaml.safe_load(merged_file.read_text())
    deployments = content.get("deployments", [])
    for d in deployments:
        if d.get("name") == deployment_name:
            return d
    raise KeyError(f"Deployment '{deployment_name}' not found in {merged_file}")


def validate_schedule(merged_file: Path, deployment_name: str) -> None:
    """Validate schedule configuration for a deployment.

    Raises ValueError describing the first problem found.
    """
    logger.info("Validating schedule config for '%s'...", deployment_name)
    config = load_deployment_config(merged_file, deployment_name)

    if "schedule" in config:
        raise ValueError(f"[{deployment_name}] {SCHEDULE_ERRORS['singular_key']}")

    schedules = config.get("schedules")
    if schedules is None:
        logger.info("\tNo schedules defined — OK")
        return

    if not schedules:
        raise ValueError(f"[{deployment_name}] {SCHEDULE_ERRORS['empty_schedules']}")

    missing_cron = [s for s in schedules if "cron" not in s]
    if missing_cron:
        raise ValueError(
            f"[{deployment_name}] "
            + SCHEDULE_ERRORS["missing_cron"].format(count=len(missing_cron))
        )


def apply_dev_overrides(
    merged_file: Path,
    deployment_name: str,
    dev_prefix: str,
    dev_work_pool: str,
) -> None:
    """Mutate the merged YAML in-place: prefix the deployment name and set the dev work pool."""
    content = yaml.safe_load(merged_file.read_text())
    for d in content.get("deployments", []):
        if d.get("name") == deployment_name:
            d["name"] = f"{dev_prefix}{deployment_name}"
            d.setdefault("work_pool", {})["name"] = dev_work_pool
            break
    merged_file.write_text(
        yaml.dump(content, default_flow_style=False, allow_unicode=True)
    )
    logger.info(
        "[DEV] Name prefixed with '%s', work pool set to '%s'.",
        dev_prefix,
        dev_work_pool,
    )


def set_git_clone_branch(merged_file: Path, branch: str) -> None:
    """Set git_clone.branch in all pull steps of the merged YAML in-place."""
    content = yaml.safe_load(merged_file.read_text())
    for step in content.get("pull", []):
        git_clone = step.get("prefect.deployments.steps.git_clone")
        if git_clone is not None:
            git_clone["branch"] = branch
    merged_file.write_text(
        yaml.dump(content, default_flow_style=False, allow_unicode=True)
    )


def set_schedules_active(merged_file: Path, deployment_name: str, active: bool) -> None:
    """Set all schedule entries' 'active' field for a deployment in-place."""
    content = yaml.safe_load(merged_file.read_text())
    for d in content.get("deployments", []):
        if d.get("name") == deployment_name:
            for schedule in d.get("schedules", []):
                schedule["active"] = active
            break
    merged_file.write_text(
        yaml.dump(content, default_flow_style=False, allow_unicode=True)
    )


def get_deployment_tags(merged_file: Path, deployment_name: str) -> list[str]:
    """Return the tags list for a named deployment."""
    config = load_deployment_config(merged_file, deployment_name)
    return config.get("tags", [])


def get_job_variables(merged_file: Path, deployment_name: str) -> dict[str, str]:
    """Return the work_pool.job_variables dict for a named deployment."""
    config = load_deployment_config(merged_file, deployment_name)
    return config.get("work_pool", {}).get("job_variables", {})


def has_schedules(merged_file: Path, deployment_name: str) -> bool:
    """Return True if the deployment has a non-empty schedules list."""
    config = load_deployment_config(merged_file, deployment_name)
    schedules = config.get("schedules")
    return bool(schedules)
