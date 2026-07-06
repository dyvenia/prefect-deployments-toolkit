"""Prefect REST API implementation for deploy/delete/schedule operations.

Works against both Prefect Cloud and self-hosted Prefect servers. All URL
construction derives from PREFECT_API_URL — nothing is hardcoded to
api.prefect.cloud.

Expected PREFECT_API_URL shapes:
  Cloud:       https://api.prefect.cloud/api/accounts/<id>/workspaces/<id>
  Self-hosted: https://prefect-dp-dev.company.com/api

This module is a drop-in alternative to `prefect_cli.py`: every deploy-
lifecycle function here (`deploy`, `delete_deployment`, `resume_schedule`,
`delete_schedule`) mirrors the CLI module's signature exactly, so
`deployment.py` can call either backend interchangeably via `ctx.client`.

Resolving the real flow name
-----------------------------
`entrypoint.rsplit(":", 1)[-1]` (the function name) is NOT a reliable flow
name — flows commonly override it via `@flow(name=...)`. Since we can't ask
the REST API for "the name this function was decorated with" without a
deployment already existing, we resolve it the same way `prefect deploy`
does internally: import the entrypoint module and read the `.name`
attribute off the decorated Flow object.

Two entrypoint formats are supported:
1. Dotted module path: "pkg.module:flow_fn" -> importlib.import_module(...)
2. File path: "flows/automations/multiflow.py:flow_fn" -> spec_from_file_location(...)

Other notes:
- `pull` steps are passed through to the API untouched, including Jinja
  placeholders like `{{ prefect.blocks.secret.... }}`. These are resolved
  by the Prefect worker at flow-run time, not at deploy time — same
  behavior as the CLI. Block references are intentionally NOT resolved
  client-side, for the same reason the CLI doesn't: doing so would bake
  secret values into the stored deployment record in plaintext.
- `{{ prefect.variables.NAME }}` placeholders inside `parameters` ARE
  resolved client-side (see `_resolve_variables`), since Variables are not
  sensitive by design and this mirrors what `prefect deploy` does for you
  automatically when using the CLI.
- Deployment creation relies on `POST /deployments/` upsert-on-existing-
  name behavior, matching what `prefect deploy` does internally.
"""

import base64
import importlib
import importlib.util
import logging
import os
import random
import re
import sys
import threading
import time
from pathlib import Path

import httpx
import yaml as _yaml

logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --- HTTP client / retry -------------------------------------------------

_client_lock = threading.Lock()
_client_instance: httpx.Client | None = None

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 30.0


def _get_client() -> httpx.Client:
    global _client_instance
    if _client_instance is None:
        with _client_lock:
            if _client_instance is None:
                _client_instance = httpx.Client(
                    timeout=30.0,
                    limits=httpx.Limits(
                        max_connections=40,
                        max_keepalive_connections=20,
                        keepalive_expiry=30.0,
                    ),
                )
    return _client_instance


def _auth_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get("PREFECT_API_KEY")
    auth_string = os.environ.get("PREFECT_API_AUTH_STRING")

    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif auth_string:
        encoded = base64.b64encode(auth_string.encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"

    return headers


def _base_url() -> str:
    """PREFECT_API_URL already contains the full path for both Cloud and
    self-hosted setups — normalize the trailing slash and use it directly.
    """
    return os.environ["PREFECT_API_URL"].rstrip("/")


def _is_prefect_cloud() -> bool:
    return "api.prefect.cloud" in _base_url()


def _url(path: str) -> str:
    return f"{_base_url()}{path}"


def _sleep_with_backoff(attempt: int, response: httpx.Response | None = None) -> None:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass
    delay = min(BASE_DELAY_SECONDS * (2**attempt), MAX_DELAY_SECONDS)
    jitter = random.uniform(0, delay * 0.5)
    time.sleep(delay + jitter)


def request(method: str, url: str, **kwargs) -> httpx.Response:
    """Send an HTTP request with retry on transient errors.

    Every call in this module MUST go through here (not a raw client
    call) so retry/backoff behavior is guaranteed for every deploy-related
    request, with no exceptions.
    """
    client = _get_client()
    last_exc: Exception | None = None

    for attempt in range(MAX_RETRIES):
        try:
            response = client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning(
                "Network error on %s %s (attempt %d/%d): %s — retrying...",
                method, url, attempt + 1, MAX_RETRIES, exc,
            )
            _sleep_with_backoff(attempt)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == MAX_RETRIES - 1:
                return response
            logger.warning(
                "Received %d from %s %s (attempt %d/%d) — retrying...",
                response.status_code, method, url, attempt + 1, MAX_RETRIES,
            )
            _sleep_with_backoff(attempt, response)
            continue

        return response

    if last_exc is not None:
        raise last_exc
    return response  # pragma: no cover


def close_client() -> None:
    """Explicitly close the shared client (call at process exit if needed)."""
    global _client_instance
    if _client_instance is not None:
        with _client_lock:
            if _client_instance is not None:
                _client_instance.close()
                _client_instance = None


def ui_deployment_url(deployment_id: str) -> str:
    """Return the Prefect UI link for a deployment_id."""
    base = _base_url()
    if _is_prefect_cloud():
        _, _, tail = base.partition("/api/accounts/")
        account_id, _, workspace_id = tail.partition("/workspaces/")
        return (
            f"https://app.prefect.cloud/account/{account_id}"
            f"/workspace/{workspace_id}/deployments/deployment/{deployment_id}"
        )
    ui_root = base[: -len("/api")] if base.endswith("/api") else base
    return f"{ui_root}/deployments/deployment/{deployment_id}"


# --- Generic Prefect entity lookups ---------------------------------------

def get_flow_ids_for_deployment(deployment_name: str) -> list[str]:
    """Return all flow_ids matching a deployment name."""
    logger.info("Retrieving flow IDs for deployment '%s'...", deployment_name)
    response = request(
        "POST", _url("/deployments/filter"),
        headers=_auth_headers(),
        json={"deployments": {"name": {"any_": [deployment_name]}}},
    )
    response.raise_for_status()
    flow_ids = [d["flow_id"] for d in response.json() if d.get("flow_id")]
    logger.debug("\tRetrieved flow_ids: %s", flow_ids)
    return flow_ids


def get_flow_name(flow_id: str) -> str | None:
    """Return the flow name for a given flow_id, or None if not found."""
    if not flow_id:
        return None
    response = request("GET", _url(f"/flows/{flow_id}"), headers=_auth_headers())
    response.raise_for_status()
    name = response.json().get("name")
    logger.debug("\tRetrieved flow name: %s", name)
    return name


def get_flow_id_by_name(flow_name: str) -> str | None:
    """Return the flow_id for an exact flow name, or None if no such flow exists."""
    response = request("GET", _url(f"/flows/name/{flow_name}"), headers=_auth_headers())
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["id"]


def get_deployment_id_by_name(deployment_name: str) -> str | None:
    """Return the deployment's own id for a bare deployment name, or None."""
    response = request(
        "POST", _url("/deployments/filter"),
        headers=_auth_headers(),
        json={"deployments": {"name": {"any_": [deployment_name]}}},
    )
    response.raise_for_status()
    deployments = response.json()
    return deployments[0]["id"] if deployments else None


def get_deployment_by_name(deployment_name: str) -> dict | None:
    """Return the full deployment record by bare deployment name, or None."""
    response = request(
        "POST", _url("/deployments/filter"),
        headers=_auth_headers(),
        json={"deployments": {"name": {"any_": [deployment_name]}}},
    )
    response.raise_for_status()
    deployments = response.json()
    return deployments[0] if deployments else None


def get_schedule_ids(full_deployment_name: str) -> list[str]:
    """Return schedule IDs for a deployment ('flow_name/deployment_name')."""
    deploy_name = full_deployment_name.rsplit("/", 1)[-1]
    deployment_id = get_deployment_id_by_name(deploy_name)
    if deployment_id is None:
        return []
    response = request(
        "GET", _url(f"/deployments/{deployment_id}/schedules"), headers=_auth_headers(),
    )
    response.raise_for_status()
    return [s["id"] for s in response.json()]


def get_variable(variable_name: str) -> object | None:
    """Return a Prefect Variable's value by name, or None if it doesn't exist."""
    response = request("GET", _url(f"/variables/name/{variable_name}"), headers=_auth_headers())
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["value"]


def _get_or_create_flow_id_by_name(flow_name: str) -> str:
    """Return the flow_id for flow_name, creating the flow if needed."""
    flow_id = get_flow_id_by_name(flow_name)
    if flow_id is not None:
        return flow_id
    logger.info("Flow '%s' not found — creating it...", flow_name)
    response = request("POST", _url("/flows/"), headers=_auth_headers(), json={"name": flow_name})
    response.raise_for_status()
    return response.json()["id"]


def _get_deployment_id_by_full_name(full_name: str) -> str | None:
    """Return the deployment_id for 'flow_name/deployment_name', or None."""
    response = request("GET", _url(f"/deployments/name/{full_name}"), headers=_auth_headers())
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["id"]


# --- Flow entrypoint resolution --------------------------------------------

_flow_object_cache: dict[str, object] = {}

_import_lock = threading.RLock()


def _import_module_from_entrypoint(module_part: str):
    """Import the module referenced by the entrypoint's module_part."""
    is_file_path = module_part.endswith(".py") or "/" in module_part

    if is_file_path:
        file_path = Path(module_part)
        if not file_path.exists():
            raise FileNotFoundError(
                f"Entrypoint file '{file_path}' does not exist relative to the "
                f"current working directory ('{Path.cwd()}')."
            )

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


def _resolve_flow_object(entrypoint: str):
    """Return the actual Flow object by importing the entrypoint."""
    if entrypoint in _flow_object_cache:
        return _flow_object_cache[entrypoint]

    module_part, _, function_name = entrypoint.rpartition(":")
    if not module_part or not function_name:
        raise ValueError(
            f"Entrypoint '{entrypoint}' is not in the expected "
            "'<module_or_path>:<function_name>' format."
        )

    logger.info("Importing entrypoint '%s' to resolve the flow object...", entrypoint)
    with _import_lock:
        module = _import_module_from_entrypoint(module_part)

    flow_obj = getattr(module, function_name, None)
    if flow_obj is None:
        raise AttributeError(
            f"Function '{function_name}' not found in module for entrypoint '{entrypoint}'."
        )
    if not hasattr(flow_obj, "name"):
        raise AttributeError(
            f"'{function_name}' in entrypoint '{entrypoint}' does not look like a "
            "Prefect @flow-decorated function (no '.name' attribute found)."
        )

    _flow_object_cache[entrypoint] = flow_obj
    return flow_obj


def _resolve_flow_name(entrypoint: str) -> str:
    """Return the real flow name by importing the entrypoint and reading Flow.name."""
    flow_obj = _resolve_flow_object(entrypoint)
    flow_name = flow_obj.name
    logger.debug("Resolved entrypoint '%s' -> flow name '%s'.", entrypoint, flow_name)
    return flow_name


# --- YAML spec extraction ---------------------------------------------------

def _find_deployment_spec(prefect_file: Path, deployment_name: str) -> dict:
    """Extract the matching deployment dict (and top-level pull steps)."""
    parsed = _yaml.safe_load(prefect_file.read_text())
    pull_steps = parsed.get("pull", [])
    for d in parsed.get("deployments", []):
        if d.get("name") == deployment_name:
            return {"spec": d, "pull_steps": pull_steps}
    raise ValueError(
        f"Deployment '{deployment_name}' not found in merged prefect file '{prefect_file}'."
    )


def _build_api_schedules(spec: dict) -> list[dict]:
    """Convert YAML schedule entries into the Prefect API's schedule shape."""
    api_schedules = []
    for sched in spec.get("schedules", []):
        cron_schedule = {
            "cron": sched["cron"],
            "timezone": sched.get("timezone", "UTC"),
            "day_or": sched.get("day_or", True),
        }
        api_schedules.append({
            "schedule": cron_schedule,
            "active": bool(sched.get("active", False)),
        })
    return api_schedules


# --- Variable template resolution -------------------------------------------

_VARIABLE_PATTERN = re.compile(r"\{\{\s*prefect\.variables\.([a-zA-Z0-9_\-]+)\s*\}\}")
_variable_cache: dict[str, object] = {}


def _get_cached_variable(variable_name: str):
    """Fetch a Prefect Variable's value, caching it for the life of this process."""
    if variable_name in _variable_cache:
        return _variable_cache[variable_name]
    value = get_variable(variable_name)
    if value is None:
        raise ValueError(f"Prefect variable '{variable_name}' does not exist.")
    _variable_cache[variable_name] = value
    return value


def _resolve_variables(value):
    """Recursively resolve '{{ prefect.variables.NAME }}' placeholders.

    NOTE: intentionally does NOT resolve '{{ prefect.blocks.* }}' —
    resolving blocks client-side risks baking secret values (e.g. AWS
    credentials) into the stored deployment record in plaintext. Block
    references are left untouched and resolved by the worker at runtime,
    same as `pull_steps` already are.
    """
    if isinstance(value, str):
        if not _VARIABLE_PATTERN.search(value):
            return value

        def _replace(match: re.Match) -> str:
            return str(_get_cached_variable(match.group(1)))

        return _VARIABLE_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _resolve_variables(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_variables(v) for v in value]
    return value


# --- Public deploy-lifecycle API (mirrors prefect_cli.py) -------------------

def deploy(
    deployment_name: str,
    tags: list[str],
    job_variables: dict[str, str],
    prefect_file: Path,
) -> None:
    """Create or update a single deployment via the Prefect REST API."""
    extracted = _find_deployment_spec(prefect_file, deployment_name)
    spec = extracted["spec"]
    pull_steps = extracted["pull_steps"]  # left untouched — resolved at runtime

    entrypoint = spec["entrypoint"]
    flow_obj = _resolve_flow_object(entrypoint)
    flow_name = flow_obj.name
    flow_id = _get_or_create_flow_id_by_name(flow_name)

    work_pool_name = spec.get("work_pool", {}).get("name")
    parameters = _resolve_variables(spec.get("parameters", {}))
    parameter_openapi_schema = flow_obj.parameters.model_dump(mode="json")
    schedules = _build_api_schedules(spec)

    payload = {
        "name": deployment_name,
        "flow_id": flow_id,
        "entrypoint": entrypoint,
        "work_pool_name": work_pool_name,
        "parameters": parameters,
        "parameter_openapi_schema": parameter_openapi_schema,
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
    response = request("POST", _url("/deployments/"), headers=_auth_headers(), json=payload)
    response.raise_for_status()
    deployment_id = response.json()["id"]

    logger.info("Deployment '%s' applied successfully via REST.", deployment_name)
    logger.info("View Deployment in UI: %s", ui_deployment_url(deployment_id))


def delete_deployment(full_name: str) -> None:
    """Delete a deployment by its full 'flow_name/deployment_name' identifier."""
    logger.info("Deleting deployment '%s' (REST)...", full_name)
    deployment_id = _get_deployment_id_by_full_name(full_name)
    if deployment_id is None:
        logger.info("Deployment '%s' does not exist — nothing to delete.", full_name)
        return
    response = request("DELETE", _url(f"/deployments/{deployment_id}"), headers=_auth_headers())
    response.raise_for_status()


def resume_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Resume (activate) a schedule by ID."""
    logger.info("Resuming schedule %s for '%s' (REST)...", schedule_id, full_deployment_name)
    deployment_id = _get_deployment_id_by_full_name(full_deployment_name)
    if deployment_id is None:
        logger.warning("Deployment '%s' not found — cannot resume schedule.", full_deployment_name)
        return
    response = request(
        "PATCH", _url(f"/deployments/{deployment_id}/schedules/{schedule_id}"),
        headers=_auth_headers(), json={"active": True},
    )
    response.raise_for_status()


def delete_schedule(full_deployment_name: str, schedule_id: str) -> None:
    """Delete a schedule by ID."""
    logger.info("Deleting schedule %s from '%s' (REST)...", schedule_id, full_deployment_name)
    deployment_id = _get_deployment_id_by_full_name(full_deployment_name)
    if deployment_id is None:
        logger.warning("Deployment '%s' not found — cannot delete schedule.", full_deployment_name)
        return
    response = request(
        "DELETE", _url(f"/deployments/{deployment_id}/schedules/{schedule_id}"),
        headers=_auth_headers(),
    )
    response.raise_for_status()
