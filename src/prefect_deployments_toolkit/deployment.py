"""Core logic for applying a single Prefect deployment."""

import logging
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from . import prefect_cli, prefect_rest, yaml_utils

logger = logging.getLogger(__name__)

DEV_PREFIX = "dev--"


@dataclass
class DeploymentContext:
    """All runtime configuration for the apply run."""

    deployment_names: list[str]
    enable_schedule: bool
    tag: str
    reference: str
    repo_name: str
    custom_image: str
    deployments_dir: Path
    dev_prefect_work_pool_name: str = ""
    backend: str = "cli"  # "cli" or "rest"
    enforce_unique_deployment_names: bool = False

    @property
    def is_dev(self) -> bool:
        return bool(self.dev_prefect_work_pool_name) and self.tag == "dev"

    @property
    def is_non_default_branch(self) -> bool:
        return self.reference not in {"main", "master"}

    @property
    def client(self):
        """Return the backend module (prefect_cli or prefect_rest) for this run."""
        return prefect_rest if self.backend == "rest" else prefect_cli


def _resolve_flow_name(
    deployment_name: str, ctx: DeploymentContext
) -> tuple[str, str | None]:
    """Return (flow_id, flow_name) for a deployment.

    If more than one flow currently has a deployment with this name:
      - default (enforce_unique_deployment_names=False): log a warning
        listing every duplicate flow, and proceed using the FIRST match
        returned by the API. Nothing is deleted.
      - enforce mode (enforce_unique_deployment_names=True): proceed the
        same way for now — the actual cleanup of duplicates happens AFTER
        deploy, in apply_single_deployment, once we know the flow this
        entrypoint currently resolves to.
    """
    flow_ids = prefect_rest.get_flow_ids_for_deployment(deployment_name)

    if len(flow_ids) <= 1:
        flow_id = flow_ids[0] if flow_ids else ""
        flow_name = prefect_rest.get_flow_name(flow_id) if flow_id else None
        return flow_id, flow_name

    flow_names = [prefect_rest.get_flow_name(fid) for fid in flow_ids]
    logger.warning(
        "Deployment name '%s' is currently used by %d different flows: %s. "
        "Deployment names should be globally unique.",
        deployment_name,
        len(flow_ids),
        flow_names,
    )

    if not ctx.enforce_unique_deployment_names:
        logger.warning(
            "enforce-unique-deployment-names is OFF — proceeding with flow '%s' "
            "(first match) without deleting the others. Pass "
            "--enforce-unique-deployment-names true to clean up duplicates.",
            flow_names[0],
        )
    else:
        logger.warning(
            "enforce-unique-deployment-names is ON — duplicates not matching the "
            "flow resolved from this deployment's entrypoint will be deleted after deploy."
        )

    return flow_ids[0], flow_names[0]


def _cleanup_duplicate_deployments(
    ctx: DeploymentContext,
    deployment_name: str,
    current_flow_id: str,
    current_flow_name: str,
    flow_ids: list[str],
) -> None:
    """Delete every deployment named `deployment_name` under a flow OTHER than
    current_flow_id. Only called when ctx.enforce_unique_deployment_names=True.
    """
    stale_flow_ids = [fid for fid in flow_ids if fid != current_flow_id]

    for stale_flow_id in stale_flow_ids:
        stale_flow_name = prefect_rest.get_flow_name(stale_flow_id)
        logger.warning(
            "Deleting duplicate deployment '%s' under flow '%s' (flow_id=%s) — "
            "keeping the one under current flow '%s' (flow_id=%s).",
            deployment_name,
            stale_flow_name,
            stale_flow_id,
            current_flow_name,
            current_flow_id,
        )
        ctx.client.delete_deployment(f"{stale_flow_name}/{deployment_name}")


def _build_tags(ctx: DeploymentContext, merged_file: Path, full_name: str) -> list[str]:
    tags = [ctx.tag, ctx.reference]
    tags += yaml_utils.get_deployment_tags(merged_file, full_name)
    return tags


def _build_job_variables(
    ctx: DeploymentContext,
    merged_file: Path,
    full_name: str,
) -> dict[str, str]:
    job_vars: dict[str, str] = {"name": full_name}

    if ctx.is_non_default_branch:
        base = f"/opt/prefect/{ctx.repo_name}-{ctx.reference}"
        job_vars["DBT_PROJECT_DIR"] = f"{base}/src/edp_flows/models"
        job_vars["DBT_PROFILES_DIR"] = f"{base}/src/edp_flows/models"
        job_vars["METRICS_EXPORTER_DIR"] = f"{base}/etc"

    if ctx.custom_image:
        job_vars["image"] = ctx.custom_image

    # Merge job_variables from YAML; image from YAML only wins if not already set above
    yaml_job_vars = yaml_utils.get_job_variables(merged_file, full_name)
    for key, value in yaml_job_vars.items():
        if key == "image" and "image" in job_vars:
            continue
        job_vars[key] = str(value)

    return job_vars


def _handle_schedules(
    ctx: DeploymentContext,
    merged_file: Path,
    full_name: str,
    flow_name: str,
) -> None:
    deployment_has_schedules = yaml_utils.has_schedules(merged_file, full_name)

    if not deployment_has_schedules:
        logger.info(
            "No schedules in YAML — removing any existing schedules from Prefect Cloud..."
        )
        schedule_ids = prefect_rest.get_schedule_ids(f"{flow_name}/{full_name}")
        for sid in schedule_ids:
            ctx.client.delete_schedule(f"{flow_name}/{full_name}", sid)
        if not schedule_ids:
            logger.info("No existing schedules to remove.")
        return

    if ctx.enable_schedule:
        logger.info("Resuming all schedules for '%s'...", full_name)
        schedule_ids = prefect_rest.get_schedule_ids(f"{flow_name}/{full_name}")
        for sid in schedule_ids:
            ctx.client.resume_schedule(f"{flow_name}/{full_name}", sid)
    else:
        logger.info(
            "Schedules exist in YAML but enable_schedule=false — will remain paused."
        )


def remove_deployment(
    ctx: DeploymentContext, deployment_name: str, full_name: str, flow_name: str | None
) -> None:
    """Delete a deployment that no longer exists in the YAML files."""
    logger.info("Deployment '%s' has been removed from YAML.", deployment_name)
    if flow_name:
        ctx.client.delete_deployment(f"{flow_name}/{full_name}")
    else:
        logger.info(
            "Deployment '%s' does not exist on Prefect Cloud (no matching flow). Skipping.",
            full_name,
        )


def apply_single_deployment(deployment_name: str, ctx: DeploymentContext) -> None:
    """Apply (create/update) or remove a single deployment."""
    yaml_file = yaml_utils.find_deployment_file(deployment_name, ctx.deployments_dir)
    prefix = DEV_PREFIX if ctx.is_dev else ""
    full_name = f"{prefix}{deployment_name}"

    flow_id, flow_name = _resolve_flow_name(full_name, ctx)

    if yaml_file is None:
        remove_deployment(ctx, deployment_name, full_name, flow_name)
        return

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
        merged_file = Path(tmp.name)

    try:
        yaml_utils.build_merged_prefect_file(
            ctx.deployments_dir / "prefect_base.yaml",
            yaml_file,
            merged_file,
        )

        if ctx.is_dev:
            yaml_utils.apply_dev_overrides(
                merged_file, deployment_name, DEV_PREFIX, ctx.dev_prefect_work_pool_name
            )

        yaml_utils.validate_schedule(merged_file, full_name)

        if ctx.is_non_default_branch:
            yaml_utils.set_git_clone_branch(merged_file, ctx.reference)

        if ctx.enable_schedule and yaml_utils.has_schedules(merged_file, full_name):
            yaml_utils.set_schedules_active(merged_file, full_name, active=True)

        tags = _build_tags(ctx, merged_file, full_name)
        job_vars = _build_job_variables(ctx, merged_file, full_name)
        logger.info("Tags: %s", tags)
        logger.info("Job variables: %s", job_vars)

        ctx.client.deploy(full_name, tags, job_vars, merged_file)
        time.sleep(0.01)

        # Detect flow rename: if flow_id changed post-deploy, clean up the old deployment
        new_flow_ids = prefect_rest.get_flow_ids_for_deployment(full_name)
        new_flow_id = new_flow_ids[0] if new_flow_ids else ""
        if flow_id and new_flow_id and new_flow_id != flow_id:
            logger.info(
                "Flow changed post-deploy — removing stale deployment under old flow."
            )
            ctx.client.delete_deployment(f"{flow_name}/{full_name}")
            flow_name = prefect_rest.get_flow_name(new_flow_id)

        if not flow_name and new_flow_id:
            logger.info(
                "New deployment — retrieving flow name from Prefect Cloud post-deploy..."
            )
            flow_name = prefect_rest.get_flow_name(new_flow_id) if new_flow_id else None

        # If duplicates existed under other flows and enforcement is on,
        # clean up everything except the deployment matching the current flow.
        if ctx.enforce_unique_deployment_names and flow_name and new_flow_id:
            _cleanup_duplicate_deployments(
                ctx, full_name, new_flow_id, flow_name, new_flow_ids
            )

        if flow_name:
            _handle_schedules(ctx, merged_file, full_name, flow_name)
        else:
            logger.warning(
                "Could not resolve flow name post-deploy — skipping schedule management."
            )

    finally:
        merged_file.unlink(missing_ok=True)
        time.sleep(0.01)
