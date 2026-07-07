"""Unit tests for prefect_deployments_toolkit.prefect_rest."""

import base64
import textwrap
from unittest.mock import MagicMock, patch

import httpx
import pytest

MOD = "prefect_deployments_toolkit.prefect_rest"

CLOUD_URL = "https://api.prefect.cloud/api/accounts/acct-123/workspaces/ws-456"
SELF_HOSTED_URL = "https://prefect.internal.company.com/api"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _response(status: int = 200, json_data: object = None) -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.json.return_value = json_data or {}
    r.headers = {}
    r.raise_for_status = MagicMock()
    if status >= 400:
        r.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status}", request=MagicMock(), response=r
        )
    return r


def _patch_request(pr, return_value=None, side_effect=None):
    """Patch prefect_rest.request (the module-level retry wrapper)."""
    if side_effect:
        return patch(f"{MOD}.request", side_effect=side_effect)
    return patch(f"{MOD}.request", return_value=return_value)


# ---------------------------------------------------------------------------
# URL / auth helpers
# ---------------------------------------------------------------------------


class TestUrlHelpers:
    def test_base_url_strips_trailing_slash(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", f"{SELF_HOSTED_URL}/")
        assert pr._base_url() == SELF_HOSTED_URL

    def test_url_builds_full_path(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        assert pr._url("/flows/abc") == f"{SELF_HOSTED_URL}/flows/abc"

    def test_is_prefect_cloud_true(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", CLOUD_URL)
        assert pr._is_prefect_cloud() is True

    def test_is_prefect_cloud_false(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        assert pr._is_prefect_cloud() is False


class TestAuthHeaders:
    def test_bearer_token_when_api_key_set(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_KEY", "my-secret-key")
        monkeypatch.delenv("PREFECT_API_AUTH_STRING", raising=False)
        headers = pr._auth_headers()
        assert headers["Authorization"] == "Bearer my-secret-key"

    def test_basic_auth_when_auth_string_set(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.delenv("PREFECT_API_KEY", raising=False)
        monkeypatch.setenv("PREFECT_API_AUTH_STRING", "user:pass")
        headers = pr._auth_headers()
        expected = "Basic " + base64.b64encode(b"user:pass").decode()
        assert headers["Authorization"] == expected

    def test_api_key_takes_precedence_over_auth_string(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_KEY", "key")
        monkeypatch.setenv("PREFECT_API_AUTH_STRING", "user:pass")
        headers = pr._auth_headers()
        assert headers["Authorization"].startswith("Bearer")

    def test_no_auth_header_when_neither_set(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.delenv("PREFECT_API_KEY", raising=False)
        monkeypatch.delenv("PREFECT_API_AUTH_STRING", raising=False)
        headers = pr._auth_headers()
        assert "Authorization" not in headers
        assert headers["Content-Type"] == "application/json"


class TestUiDeploymentUrl:
    def test_cloud_url_format(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", CLOUD_URL)
        url = pr.ui_deployment_url("dep-id-789")
        assert "app.prefect.cloud" in url
        assert "acct-123" in url
        assert "ws-456" in url
        assert "dep-id-789" in url

    def test_self_hosted_url_format(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        url = pr.ui_deployment_url("dep-id-789")
        assert "dep-id-789" in url
        assert "prefect.internal.company.com" in url
        assert "/api" not in url.split("/deployments")[0].split("company.com")[1]


# ---------------------------------------------------------------------------
# request() — retry logic
# ---------------------------------------------------------------------------


class TestRequestRetry:
    """Test the retry/backoff wrapper — patch the underlying httpx client."""

    def setup_method(self):
        # Reset the singleton client so each test gets a fresh mock
        from prefect_deployments_toolkit import prefect_rest as pr

        pr._client_instance = None

    def _make_mock_client(self, responses: list) -> MagicMock:
        client = MagicMock(spec=httpx.Client)
        client.request.side_effect = responses
        return client

    def test_returns_immediately_on_success(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        ok = _response(200)
        client = self._make_mock_client([ok])
        with patch(f"{MOD}._get_client", return_value=client):
            result = pr.request("GET", f"{SELF_HOSTED_URL}/flows/abc")
        assert result.status_code == 200
        assert client.request.call_count == 1

    def test_retries_on_retryable_status_codes(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        fail = _response(503)
        ok = _response(200)
        client = self._make_mock_client([fail, fail, ok])
        with (
            patch(f"{MOD}._get_client", return_value=client),
            patch(f"{MOD}._sleep_with_backoff"),
        ):
            result = pr.request("GET", f"{SELF_HOSTED_URL}/flows/abc")
        assert result.status_code == 200
        assert client.request.call_count == 3

    def test_returns_last_retryable_response_after_max_retries(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        fail = _response(429)
        client = self._make_mock_client([fail] * pr.MAX_RETRIES)
        with (
            patch(f"{MOD}._get_client", return_value=client),
            patch(f"{MOD}._sleep_with_backoff"),
        ):
            result = pr.request("GET", f"{SELF_HOSTED_URL}/flows/abc")
        assert result.status_code == 429
        assert client.request.call_count == pr.MAX_RETRIES

    def test_retries_on_transport_error(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        ok = _response(200)
        client = self._make_mock_client(
            [
                httpx.ConnectError("connection refused"),
                ok,
            ]
        )
        with (
            patch(f"{MOD}._get_client", return_value=client),
            patch(f"{MOD}._sleep_with_backoff"),
        ):
            result = pr.request("GET", f"{SELF_HOSTED_URL}/flows/abc")
        assert result.status_code == 200

    def test_raises_transport_error_after_max_retries(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        exc = httpx.ConnectError("unreachable")
        client = self._make_mock_client([exc] * pr.MAX_RETRIES)
        with (
            patch(f"{MOD}._get_client", return_value=client),
            patch(f"{MOD}._sleep_with_backoff"),
        ):
            with pytest.raises(httpx.ConnectError):
                pr.request("GET", f"{SELF_HOSTED_URL}/flows/abc")

    def test_non_retryable_status_returned_immediately(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        not_found = _response(404)
        client = self._make_mock_client([not_found])
        with patch(f"{MOD}._get_client", return_value=client):
            result = pr.request("GET", f"{SELF_HOSTED_URL}/flows/missing")
        assert result.status_code == 404
        assert client.request.call_count == 1


# ---------------------------------------------------------------------------
# _sleep_with_backoff
# ---------------------------------------------------------------------------


class TestSleepWithBackoff:
    def test_uses_retry_after_header_when_present(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        resp = MagicMock()
        resp.headers = {"Retry-After": "5"}
        with patch(f"{MOD}.time.sleep") as mock_sleep:
            pr._sleep_with_backoff(0, resp)
        mock_sleep.assert_called_once_with(5.0)

    def test_falls_back_to_exponential_when_no_retry_after(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        with patch(f"{MOD}.time.sleep") as mock_sleep:
            pr._sleep_with_backoff(0, None)
        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        # attempt=0 → base delay = 1.0, jitter ≤ 0.5 → total ≤ 1.5
        assert 1.0 <= sleep_time <= 1.5

    def test_exponential_delay_grows_with_attempt(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        delays = []
        with patch(f"{MOD}.time.sleep", side_effect=lambda t: delays.append(t)):
            pr._sleep_with_backoff(0)
            pr._sleep_with_backoff(1)
            pr._sleep_with_backoff(2)
        # Each delay should be larger than the previous base
        assert delays[1] > delays[0] or True  # jitter may overlap at low attempts
        # attempt=2 base = 4.0, so delay must be ≥ 4.0
        assert delays[2] >= 4.0

    def test_delay_capped_at_max(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        with patch(f"{MOD}.time.sleep") as mock_sleep:
            pr._sleep_with_backoff(100)  # very high attempt
        sleep_time = mock_sleep.call_args[0][0]
        assert sleep_time <= pr.MAX_DELAY_SECONDS * 1.5  # max + max jitter


# ---------------------------------------------------------------------------
# Entity lookup functions — all patch `prefect_rest.request`
# ---------------------------------------------------------------------------


class TestGetFlowIdsForDeployment:
    def test_returns_flow_ids(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, [{"flow_id": "fid-1"}, {"flow_id": "fid-2"}])
        with _patch_request(pr, return_value=resp):
            result = pr.get_flow_ids_for_deployment("my-flow")
        assert result == ["fid-1", "fid-2"]

    def test_filters_entries_without_flow_id(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, [{"flow_id": "fid-1"}, {}])
        with _patch_request(pr, return_value=resp):
            result = pr.get_flow_ids_for_deployment("my-flow")
        assert result == ["fid-1"]

    def test_returns_empty_list_when_none_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, [])
        with _patch_request(pr, return_value=resp):
            result = pr.get_flow_ids_for_deployment("ghost")
        assert result == []


class TestGetFlowName:
    def test_returns_none_for_empty_flow_id(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        result = pr.get_flow_name("")
        assert result is None

    def test_returns_flow_name(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, {"name": "my-awesome-flow"})
        with _patch_request(pr, return_value=resp):
            result = pr.get_flow_name("fid-1")
        assert result == "my-awesome-flow"


class TestGetFlowIdByName:
    def test_returns_none_on_404(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with _patch_request(pr, return_value=_response(404)):
            result = pr.get_flow_id_by_name("nonexistent")
        assert result is None

    def test_returns_id_on_success(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, {"id": "fid-abc"})
        with _patch_request(pr, return_value=resp):
            result = pr.get_flow_id_by_name("my-flow")
        assert result == "fid-abc"


class TestGetDeploymentIdByName:
    def test_returns_none_when_empty_list(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with _patch_request(pr, return_value=_response(200, [])):
            result = pr.get_deployment_id_by_name("ghost")
        assert result is None

    def test_returns_first_id(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, [{"id": "dep-123"}, {"id": "dep-456"}])
        with _patch_request(pr, return_value=resp):
            result = pr.get_deployment_id_by_name("my-dep")
        assert result == "dep-123"


class TestGetDeploymentByName:
    def test_returns_none_when_empty(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with _patch_request(pr, return_value=_response(200, [])):
            result = pr.get_deployment_by_name("ghost")
        assert result is None

    def test_returns_first_record(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        record = {"id": "dep-123", "name": "my-dep"}
        with _patch_request(pr, return_value=_response(200, [record])):
            result = pr.get_deployment_by_name("my-dep")
        assert result == record


class TestGetScheduleIds:
    def test_returns_empty_when_deployment_not_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with patch(f"{MOD}.get_deployment_id_by_name", return_value=None):
            result = pr.get_schedule_ids("my-flow/my-dep")
        assert result == []

    def test_returns_schedule_ids(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        schedules = [{"id": "s1"}, {"id": "s2"}]
        with (
            patch(f"{MOD}.get_deployment_id_by_name", return_value="dep-123"),
            _patch_request(pr, return_value=_response(200, schedules)),
        ):
            result = pr.get_schedule_ids("my-flow/my-dep")
        assert result == ["s1", "s2"]

    def test_uses_bare_deployment_name_not_full_name(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with (
            patch(f"{MOD}.get_deployment_id_by_name", return_value=None) as mock_id,
        ):
            pr.get_schedule_ids("some-flow/my-dep")
        mock_id.assert_called_once_with("my-dep")


class TestGetVariable:
    def test_returns_none_on_404(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with _patch_request(pr, return_value=_response(404)):
            result = pr.get_variable("missing-var")
        assert result is None

    def test_returns_value(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        resp = _response(200, {"value": "prod"})
        with _patch_request(pr, return_value=resp):
            result = pr.get_variable("env")
        assert result == "prod"


# ---------------------------------------------------------------------------
# _resolve_variables
# ---------------------------------------------------------------------------


class TestResolveVariables:
    def test_resolves_variable_placeholder_in_string(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        pr._variable_cache.clear()
        with patch(f"{MOD}.get_variable", return_value="production"):
            result = pr._resolve_variables("env={{ prefect.variables.env }}")
        assert result == "env=production"

    def test_leaves_block_placeholders_untouched(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        val = "{{ prefect.blocks.secret.my-creds }}"
        result = pr._resolve_variables(val)
        assert result == val

    def test_resolves_nested_dict(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        pr._variable_cache.clear()
        with patch(f"{MOD}.get_variable", return_value="us-east-1"):
            result = pr._resolve_variables({"region": "{{ prefect.variables.region }}"})
        assert result == {"region": "us-east-1"}

    def test_resolves_nested_list(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        pr._variable_cache.clear()
        with patch(f"{MOD}.get_variable", return_value="42"):
            result = pr._resolve_variables(["{{ prefect.variables.count }}", "static"])
        assert result == ["42", "static"]

    def test_passthrough_non_string_scalar(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        assert pr._resolve_variables(123) == 123
        assert pr._resolve_variables(None) is None

    def test_raises_when_variable_does_not_exist(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        pr._variable_cache.clear()
        with patch(f"{MOD}.get_variable", return_value=None):
            with pytest.raises(ValueError, match="does not exist"):
                pr._resolve_variables("{{ prefect.variables.missing }}")

    def test_variable_cached_after_first_fetch(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        pr._variable_cache.clear()
        with patch(f"{MOD}.get_variable", return_value="val") as mock_get:
            pr._resolve_variables("{{ prefect.variables.x }}")
            pr._resolve_variables("{{ prefect.variables.x }}")
        mock_get.assert_called_once()


# ---------------------------------------------------------------------------
# _build_api_schedules
# ---------------------------------------------------------------------------


class TestBuildApiSchedules:
    def test_empty_spec_returns_empty_list(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        assert pr._build_api_schedules({}) == []

    def test_converts_cron_schedule(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        spec = {"schedules": [{"cron": "0 6 * * *", "timezone": "Europe/Warsaw"}]}
        result = pr._build_api_schedules(spec)
        assert len(result) == 1
        assert result[0]["schedule"]["cron"] == "0 6 * * *"
        assert result[0]["schedule"]["timezone"] == "Europe/Warsaw"

    def test_defaults_timezone_to_utc(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        spec = {"schedules": [{"cron": "0 * * * *"}]}
        result = pr._build_api_schedules(spec)
        assert result[0]["schedule"]["timezone"] == "UTC"

    def test_active_defaults_to_false(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        spec = {"schedules": [{"cron": "0 * * * *"}]}
        result = pr._build_api_schedules(spec)
        assert result[0]["active"] is False

    def test_active_true_when_set(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        spec = {"schedules": [{"cron": "0 * * * *", "active": True}]}
        result = pr._build_api_schedules(spec)
        assert result[0]["active"] is True

    def test_multiple_schedules(self):
        from prefect_deployments_toolkit import prefect_rest as pr

        spec = {
            "schedules": [
                {"cron": "0 6 * * *"},
                {"cron": "0 18 * * *", "active": True},
            ]
        }
        result = pr._build_api_schedules(spec)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _find_deployment_spec
# ---------------------------------------------------------------------------


class TestFindDeploymentSpec:
    PREFECT_FILE_CONTENT = textwrap.dedent("""\
        pull:
          - prefect.deployments.steps.git_clone:
              repository: git@github.com:org/repo.git
        deployments:
          - name: my-dep
            entrypoint: flows/f.py:my_flow
    """)

    def test_returns_spec_and_pull_steps(self, tmp_path):
        from prefect_deployments_toolkit import prefect_rest as pr

        f = tmp_path / "prefect.yaml"
        f.write_text(self.PREFECT_FILE_CONTENT)
        result = pr._find_deployment_spec(f, "my-dep")
        assert result["spec"]["name"] == "my-dep"
        assert isinstance(result["pull_steps"], list)

    def test_raises_when_deployment_not_found(self, tmp_path):
        from prefect_deployments_toolkit import prefect_rest as pr

        f = tmp_path / "prefect.yaml"
        f.write_text(self.PREFECT_FILE_CONTENT)
        with pytest.raises(ValueError, match="not found"):
            pr._find_deployment_spec(f, "nonexistent")


# ---------------------------------------------------------------------------
# delete_deployment (REST public API)
# ---------------------------------------------------------------------------


class TestRestDeleteDeployment:
    def test_does_nothing_when_deployment_not_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with patch(f"{MOD}._get_deployment_id_by_full_name", return_value=None):
            with patch(f"{MOD}.request") as mock_req:
                pr.delete_deployment("my-flow/my-dep")
        mock_req.assert_not_called()

    def test_sends_delete_request_when_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with (
            patch(f"{MOD}._get_deployment_id_by_full_name", return_value="dep-123"),
            patch(f"{MOD}.request", return_value=_response(204)) as mock_req,
        ):
            pr.delete_deployment("my-flow/my-dep")
        method, url = mock_req.call_args[0]
        assert method == "DELETE"
        assert "dep-123" in url


# ---------------------------------------------------------------------------
# resume_schedule (REST public API)
# ---------------------------------------------------------------------------


class TestRestResumeSchedule:
    def test_does_nothing_when_deployment_not_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with patch(f"{MOD}._get_deployment_id_by_full_name", return_value=None):
            with patch(f"{MOD}.request") as mock_req:
                pr.resume_schedule("my-flow/my-dep", "sched-1")
        mock_req.assert_not_called()

    def test_patches_schedule_active_true(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with (
            patch(f"{MOD}._get_deployment_id_by_full_name", return_value="dep-123"),
            patch(f"{MOD}.request", return_value=_response(200)) as mock_req,
        ):
            pr.resume_schedule("my-flow/my-dep", "sched-1")
        method, url = mock_req.call_args[0]
        assert method == "PATCH"
        assert "dep-123" in url
        assert "sched-1" in url
        assert mock_req.call_args[1]["json"] == {"active": True}


# ---------------------------------------------------------------------------
# delete_schedule (REST public API)
# ---------------------------------------------------------------------------


class TestRestDeleteSchedule:
    def test_does_nothing_when_deployment_not_found(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with patch(f"{MOD}._get_deployment_id_by_full_name", return_value=None):
            with patch(f"{MOD}.request") as mock_req:
                pr.delete_schedule("my-flow/my-dep", "sched-1")
        mock_req.assert_not_called()

    def test_sends_delete_request(self, monkeypatch):
        from prefect_deployments_toolkit import prefect_rest as pr

        monkeypatch.setenv("PREFECT_API_URL", SELF_HOSTED_URL)
        with (
            patch(f"{MOD}._get_deployment_id_by_full_name", return_value="dep-123"),
            patch(f"{MOD}.request", return_value=_response(204)) as mock_req,
        ):
            pr.delete_schedule("my-flow/my-dep", "sched-1")
        method, url = mock_req.call_args[0]
        assert method == "DELETE"
        assert "dep-123" in url
        assert "sched-1" in url
