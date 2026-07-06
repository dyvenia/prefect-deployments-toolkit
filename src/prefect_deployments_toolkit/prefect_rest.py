"""Prefect Cloud REST API implementation for deploy/delete/schedule operations.

This module is a drop-in alternative to `prefect_cli.py`: every public
function here mirrors the CLI module's signature exactly, so `deployment.py`
can call either backend interchangeably.

Resolving the real flow name
-----------------------------
`entrypoint.rsplit(":", 1)[-1]` (the function name) is NOT a reliable flow
name — flows commonly override it via `@flow(name=...)`, e.g.:

    @flow(name="extract--sap--parquet")
    def sap_to_parquet(...):

Since we can't ask the REST API for "the name this function was decorated
with" without a deployment already existing, we resolve it the same way
`prefect deploy` does internally: import the entrypoint module and read the
`.name` attribute off the decorated Flow object. This is exact — no
guessing, no reliance on prior deployments.

Two entrypoint formats are supported:
  1. Dotted module path:  "viadot.orchestration.prefect.flows.sql_server_to_parquet:sql_server_to_parquet"
     -> imported via importlib.import_module(...)
  2. File path:           "flows/automations/multiflow.py:multiflow"
     -> imported via importlib.util.spec_from_file_location(...)

Note: this imports the flow's Python module (and therefore `prefect`, plus
whatever `viadot`/other libraries it depends on) into the current process.
It does NOT invoke the `prefect` CLI as a subprocess — no CLI dependency is
introduced by this module.

Other notes:
- `pull` steps are passed through to the API untouched, including Jinja
  placeholders like `{{ prefect.blocks.secret.... }}`. These are resolved
  by the Prefect worker at flow-run time, not at deploy time — same
  behavior as the CLI.
- Deployment creation relies on the Prefect Cloud API's upsert behavior:
  `POST /deployments/` creates or updates a deployment for a given
  `flow_id` + `name` pair, matching what `prefect deploy` does internally.
"""

import importlib
import importlib.util
import logging
import sys
import threading
from pathlib import Path

import yaml as _yaml

from . import prefect_api

logger = logging.getLogger(__name__)

_flow_name_cache: dict[str, str] = {}  # entrypoint -> resolved flow name

# Serializes ALL entrypoint imports process-wide. CPython's per-module import
# lock can deadlock when multiple threads concurrently import modules that
# have interdependent sibling imports (e.g. a package whose modules import
# each other) in different orders. This is not specific to any one package —
# it can happen with any third-party or first-party module tree. Forcing a
# single thread through import_module()/exec_module() at a time eliminates
# the deadlock entirely, at negligible cost since sys.modules caches imports
# after the first successful call.
_import_lock = threading.RLock()

def _client():
    return prefect_api.get_client()


def _headers() -> dict[str, str]:
    return prefect_api.auth_headers()


def _url(path: str) -> str:
    return f"{prefect_api.base_url()}{path}"


def _import_module_from_entrypoint(module_part: str):
    """Import the module referenced by the entrypoint's module_part.

    Supports both dotted module paths ("pkg.sub.module") and file paths
    ("flows/automations/multiflow.py").
    """
    is_file_path = module_part.endswith(".py") or "/" in module_part

    if is_file_path:
        file_path = Path(module_part)
        if not file_path.exists():
            raise FileNotFoundError(
                f"Entrypoint file '{file_path}' does not exist relative to the "
                f"current working directory ('{Path.cwd()}')."
            )
        
        # Some flow files import sibling packages living alongside them or at
        # the repo root (e.g. "import custom_flows"). importlib's
        # spec_from_file_location does NOT add these to sys.path on its own,
        # so intra-repo imports fail even though the same file imports fine
        # when run as part of `prefect deploy` / the deployed runtime, where
        # the repo layout is on sys.path already. Add both directories here
        # to match that behavior.
        repo_root = str(Path.cwd())
        file_dir = str(file_path.parent.resolve())
        for path_entry in (repo_root, file_dir):
            if path_entry not in sys.path:
                sys.path.insert(0, path_entry)

        module_name = file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not build an import spec for '{file_path}'.")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    return importlib.import_module(module_part)


def _resolve_flow_name(entrypoint: str) -> str:
    """Return the real flow name by importing the entrypoint and reading Flow.name.

    entrypoint format: "<module_path_or_file>:<function_name>"
    """
    if entrypoint in _flow_name_cache:
        return _flow_name_cache[entrypoint]

    module_part, _, function_name = entrypoint.rpartition(":")
    if not module_part or not function_name:
        raise ValueError(
            f"Entrypoint '{entrypoint}' is not in the expected "
            "'<module_or_file>:<function_name>' format."
        )

    logger.info("Importing entrypoint '%s' to resolve the real flow name...", entrypoint)
    with _import_lock:
        module = _import_module_from_entrypoint(module_part)

    flow_obj = getattr(module, function_name, None)
    if flow_obj is None:
        raise AttributeError(
            f"Function '{function_name}' not found in module for entrypoint '{entrypoint}'."
        )

    flow_name = getattr(flow_obj, "name", None)
    if not flow_name:
        raise AttributeError(
            f"'{function_name}' in entrypoint '{entrypoint}' does not look like a "
            "Prefect @flow-decorated function (no '.name' attribute found)."
        )

    logger.debug("Resolved entrypoint '%s' -> flow name '%s'.", entrypoint, flow_name)
    _flow_name_cache[entrypoint] = flow_name
    return flow_name


def _get_or_create_flow_id_by_name(flow_name: str) -> str:
    """Return the flow_id for flow_name, creating the flow in Prefect Cloud if needed."""
    response = _client().get(_url(f"/flows/name/{flow_name}"), headers=_headers())
    if response.status_code == 200:
        return response.json()["id"]
    if response.status_code != 404:
        response.raise_for_status()

    logger.info("Flow '%s' not found — creating it...", flow_name)
    create_response = _client().post(
        _url("/flows/"),
        headers=_headers(),
        json={"name": flow_name},
    )
    create_response.raise_for_status()
    return create_response.json()["id"]


def _get_deployment_id_by_full_name(full_name: str) -> str | None:
    """Return the deployment_id for 'flow_name/deployment_name', or None if not found."""
    response = _client().get(_url(f"/deployments/name/{full_name}"), headers=_headers())
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["id"]


def _find_deployment_spec(prefect_file: Path, deployment_name: str) -> dict:
    """Extract the matching deployment dict (and top-level pull steps) from the merged YAML."""
    parsed = _yaml.safe_load(prefect_file.read_text())
    pull_steps = parsed.get("pull", [])
    for d in parsed.get("deployments", []):
        if d.get("name") == deployment_name:
            return {"spec": d, "pull_steps": pull_steps}
    raise ValueError(
        f"Deployment '{deployment_name}' not found in merged prefect file '{prefect_file}'."
    )


def _build_api_schedules(spec: dict) -> list[dict]:
    """Convert YAML schedule entries into the Prefect API's DeploymentScheduleCreate shape."""
    api_schedules = []
    for sched in spec.get("schedules", []):
        cron_schedule = {
            "cron": sched["cron"],
            "timezone": sched.get("timezone", "UTC"),
            "day_or": sched.get("day_or", True),
        }
        api_schedules.append(
            {
                "schedule": cron_schedule,
                "active": bool(sched.get("active", False)),
            }
        )
    return api_schedules


def deploy(
    deployment_name: str,
    tags: list[str],
    job_variables: dict[str, str],
    prefect_file: Path,
) -> None:
    """Create or update a single deployment via the Prefect Cloud REST API."""
    extracted = _find_deployment_spec(prefect_file, deployment_name)
    spec = extracted["spec"]
    pull_steps = extracted["pull_steps"]

    entrypoint = spec["entrypoint"]
    flow_name = _resolve_flow_name(entrypoint)
    flow_id = _get_or_create_flow_id_by_name(flow_name)

    work_pool_name = spec.get("work_pool", {}).get("name")
    parameters = spec.get("parameters", {})
    schedules = _build_api_schedules(spec)

    payload = {
        "name": deployment_name,
        "flow_id": flow_id,
        "entrypoint": entrypoint,
        "work_pool_name": work_pool_name,
        "parameters": parameters,
        "job_variables": job_variables,
        "tags": tags,
        "pull_steps": pull_steps,
        "schedules": schedules,
        "enforce_parameter_schema": False,
    }

    logger.info(
        "Deploying '%s' (flow_name=%s, flow_id=%s) via REST API...",
        deployment_name, flow_name, flow_id,
    )
    response = _client().post(_url("/deployments/"), headers=_headers(), json=payload)
    response.raise_for_status()
    deployment_id = response.json()["id"]

    logger.info("Deployment '%s' applied successfully via REST.", deployment_name)
    logger.info(
        "View Deployment in UI: %s",
        prefect_api.ui_deployment_url(deployment_id),
    )


def delete_deployment(full_name: str) -> None:
    """Delete a deployment by its full 'flow_name/deployment_name' identifier."""
    logger.info("Deleting deployment '%s' from Prefect Cloud (REST)...", full_name)
    deployment_id = _get_deployment_id_by_full_name(full_name)
    if deployment_id is None:
        logger.info("Deployment '%s' does not exist — nothing to delete.", full_name)
        return
    response = _client().delete(_url(f"/deployments/{deployment_id}"), headers=_headers())
    response.raise_for_status()


def resume_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Resume (activate) a schedule by ID."""
    logger.info("Resuming schedule %s for '%s' (REST)...", schedule_id, full_deployment_name)
    deployment_id = _get_deployment_id_by_full_name(full_deployment_name)
    if deployment_id is None:
        logger.warning("Deployment '%s' not found — cannot resume schedule.", full_deployment_name)
        return
    response = _client().patch(
        _url(f"/deployments/{deployment_id}/schedules/{schedule_id}"),
        headers=_headers(),
        json={"active": True},
    )
    response.raise_for_status()


def delete_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Delete a schedule by ID."""
    logger.info("Deleting schedule %s from '%s' (REST)...", schedule_id, full_deployment_name)
    deployment_id = _get_deployment_id_by_full_name(full_deployment_name)
    if deployment_id is None:
        logger.warning("Deployment '%s' not found — cannot delete schedule.", full_deployment_name)
        return
    response = _client().delete(
        _url(f"/deployments/{deployment_id}/schedules/{schedule_id}"),
        headers=_headers(),
    )
    response.raise_for_status()
