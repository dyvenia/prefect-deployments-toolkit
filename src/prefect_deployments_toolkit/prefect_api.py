"""Prefect Cloud REST API helpers with a shared connection pool and retry logic.

Works against both Prefect Cloud and self-hosted Prefect servers. All URL
construction derives from PREFECT_API_URL — nothing is hardcoded to
api.prefect.cloud.

Expected PREFECT_API_URL shapes:
  Cloud:        https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>
  Self-hosted:  https://prefect-dp-dev.company.com/api
"""

import logging
import os
import random
import threading
import time

import httpx

logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_client_lock = threading.Lock()
_client: httpx.Client | None = None

# --- Retry configuration -----------------------------------------------
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 30.0


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = httpx.Client(
                    timeout=30.0,
                    limits=httpx.Limits(
                        max_connections=40,
                        max_keepalive_connections=20,
                        keepalive_expiry=30.0,
                    ),
                )
    return _client


def _auth_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get("PREFECT_API_KEY")
    auth_string = os.environ.get("PREFECT_API_AUTH_STRING")

    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif auth_string:
        import base64
        encoded = base64.b64encode(auth_string.encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"

    return headers


def _base_url() -> str:
    """Return the API base URL exactly as configured.

    PREFECT_API_URL already contains the full path needed for both Cloud
    (.../accounts/<id>/workspaces/<id>) and self-hosted (.../api) — so we
    just normalize the trailing slash and use it directly, with no
    hardcoded host or reconstruction from other env vars.
    """
    return os.environ["PREFECT_API_URL"].rstrip("/")


def _is_prefect_cloud() -> bool:
    return "api.prefect.cloud" in _base_url()


def _sleep_with_backoff(attempt: int, response: httpx.Response | None = None) -> None:
    """Sleep before the next retry attempt.

    Honors a Retry-After header when present (common on 429/503 from
    Prefect Cloud); otherwise falls back to exponential backoff with
    jitter so concurrent workers don't all retry in lockstep.
    """
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass  # non-numeric Retry-After — fall through to backoff

    delay = min(BASE_DELAY_SECONDS * (2**attempt), MAX_DELAY_SECONDS)
    jitter = random.uniform(0, delay * 0.5)
    time.sleep(delay + jitter)


def request(method: str, url: str, **kwargs) -> httpx.Response:
    """Send an HTTP request with retry on transient errors.

    Retries on:
      - httpx.TransportError (connection resets, timeouts, DNS blips)
      - Retryable status codes (429, 500, 502, 503, 504)

    Does NOT call raise_for_status() — callers keep doing that themselves
    on the returned response, exactly as before. If all retries are
    exhausted, the last response (or exception) is returned/raised so the
    existing error-handling/logging in callers is unaffected.
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
                return response  # exhausted — let caller's raise_for_status surface it
            logger.warning(
                "Received %d from %s %s (attempt %d/%d) — retrying...",
                response.status_code, method, url, attempt + 1, MAX_RETRIES,
            )
            _sleep_with_backoff(attempt, response)
            continue

        return response

    if last_exc is not None:
        raise last_exc
    return response  # pragma: no cover — unreachable, satisfies type checkers


# Public aliases — used by prefect_rest.py so it doesn't need to reach into
# this module's private helpers. Keeps the shared connection pool/auth/base
# URL/retry logic defined in exactly one place.
def get_client() -> httpx.Client:
    """Return the shared httpx client (public alias of _get_client)."""
    return _get_client()


def auth_headers() -> dict[str, str]:
    """Return the auth headers dict (public alias of _auth_headers)."""
    return _auth_headers()


def base_url() -> str:
    """Return the workspace/server API base URL (public alias of _base_url)."""
    return _base_url()


def ui_deployment_url(deployment_id: str) -> str:
    """Return the Prefect UI link for a deployment_id.

    Mirrors the "View Deployment in UI" line the CLI prints after
    `prefect deploy`. Handles both Prefect Cloud and self-hosted UI shapes:

      Cloud:        https://app.prefect.cloud/account/<id>/workspace/<id>/deployments/deployment/<deployment_id>
      Self-hosted:  https://prefect-dp-dev.company.com/deployments/deployment/<deployment_id>
    """
    base = _base_url()

    if _is_prefect_cloud():
        # base looks like: https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>
        _, _, tail = base.partition("/api/accounts/")
        account_id, _, workspace_id = tail.partition("/workspaces/")
        return (
            f"https://app.prefect.cloud/account/{account_id}"
            f"/workspace/{workspace_id}/deployments/deployment/{deployment_id}"
        )

    # Self-hosted: strip a trailing "/api" to get the UI host, then append the path.
    ui_root = base[: -len("/api")] if base.endswith("/api") else base
    return f"{ui_root}/deployments/deployment/{deployment_id}"


def get_flow_ids_for_deployment(deployment_name: str) -> list[str]:
    """Return all flow_ids matching a deployment name from Prefect Cloud."""
    logger.info("Retrieving flow IDs for deployment '%s'...", deployment_name)
    response = request(
        "POST",
        f"{_base_url()}/deployments/filter",
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
    response = request(
        "GET",
        f"{_base_url()}/flows/{flow_id}",
        headers=_auth_headers(),
    )
    response.raise_for_status()
    name = response.json().get("name")
    logger.debug("\tRetrieved flow name: %s", name)
    return name


def get_flow_id_by_name(flow_name: str) -> str | None:
    """Return the flow_id for an exact flow name, or None if no such flow exists."""
    response = request(
        "GET",
        f"{_base_url()}/flows/name/{flow_name}",
        headers=_auth_headers(),
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["id"]


def get_deployment_id_by_name(deployment_name: str) -> str | None:
    """Return the deployment_id for a bare deployment name, or None if not found.

    Unlike get_flow_ids_for_deployment, this returns the deployment's own id
    (not the flow_id) — used for building UI links and for schedule/delete
    operations that need the deployment_id directly.
    """
    response = request(
        "POST",
        f"{_base_url()}/deployments/filter",
        headers=_auth_headers(),
        json={"deployments": {"name": {"any_": [deployment_name]}}},
    )
    response.raise_for_status()
    deployments = response.json()
    return deployments[0]["id"] if deployments else None


def get_deployment_by_name(deployment_name: str) -> dict | None:
    """Return the full deployment record (id, flow_id, name, ...) by bare
    deployment name, regardless of which flow it currently belongs to, or
    None if no deployment with that name exists.

    Assumes deployment names are globally unique across all flows in the
    workspace — used to detect deployments whose underlying flow was renamed
    (e.g. deployment1/flow1 -> deployment1/flow2) so the stale record under
    the old flow can be cleaned up.
    """
    response = request(
        "POST",
        f"{_base_url()}/deployments/filter",
        headers=_auth_headers(),
        json={"deployments": {"name": {"any_": [deployment_name]}}},
    )
    response.raise_for_status()
    deployments = response.json()
    return deployments[0] if deployments else None


def get_schedule_ids(full_deployment_name: str) -> list[str]:
    """Return schedule IDs for a deployment.

    full_deployment_name should be in the format 'flow_name/deployment_name'.
    """
    deploy_name = full_deployment_name.rsplit("/", 1)[-1]
    deployment_id = get_deployment_id_by_name(deploy_name)
    if deployment_id is None:
        return []
    sched_response = request(
        "GET",
        f"{_base_url()}/deployments/{deployment_id}/schedules",
        headers=_auth_headers(),
    )
    sched_response.raise_for_status()
    return [s["id"] for s in sched_response.json()]


def get_variable(variable_name: str) -> object | None:
    """Return a Prefect Variable's value by name, or None if it doesn't exist."""
    response = request(
        "GET",
        f"{_base_url()}/variables/name/{variable_name}",
        headers=_auth_headers(),
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["value"]

def close_client() -> None:
    """Explicitly close the shared client (call at process exit if needed)."""
    global _client
    if _client is not None:
        with _client_lock:
            if _client is not None:
                _client.close()
                _client = None
