"""Unit tests for prefect_deployments_toolkit.deployment_loader."""

import io
import subprocess
import tarfile
import textwrap
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_YAML = textwrap.dedent("""\
    pull:
      - prefect.deployments.steps.git_clone:
          repository: git@github.com:org/repo.git
          branch: main
""")

DEP_A_YAML = textwrap.dedent("""\
    deployments:
      - name: flow-a
        entrypoint: flows/a.py:flow_a
        tags:
          - etl
        pull:
          - step: something
""")

DEP_B_YAML = textwrap.dedent("""\
    deployments:
      - name: flow-b
        entrypoint: flows/b.py:flow_b
""")

INVALID_YAML = "deployments: [\n  unclosed bracket"

MISSING_NAME_YAML = textwrap.dedent("""\
    deployments:
      - entrypoint: flows/a.py:flow_a
""")

NON_DICT_ENTRY_YAML = textwrap.dedent("""\
    deployments:
      - just a string
""")

LIST_FORMAT_YAML = textwrap.dedent("""\
    - name: flow-list
      entrypoint: flows/l.py:flow_l
""")

NO_DEPLOYMENTS_KEY_YAML = textwrap.dedent("""\
    something_else:
      - foo: bar
""")


def _make_tar(files: dict[str, str]) -> bytes:
    """Build an in-memory tar archive from a dict of {path: content}."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in files.items():
            encoded = content.encode()
            info = tarfile.TarInfo(name=name)
            info.size = len(encoded)
            tar.addfile(info, io.BytesIO(encoded))
    return buf.getvalue()


# ---------------------------------------------------------------------------
# normalize_deployment
# ---------------------------------------------------------------------------


class TestNormalizeDeployment:
    def test_removes_pull_key(self):
        from prefect_deployments_toolkit.deployment_loader import normalize_deployment

        dep = {"name": "x", "entrypoint": "f.py:f", "pull": [{"step": "git_clone"}]}
        result = normalize_deployment(dep)
        assert "pull" not in result
        assert result["name"] == "x"

    def test_keeps_all_other_keys(self):
        from prefect_deployments_toolkit.deployment_loader import normalize_deployment

        dep = {"name": "x", "entrypoint": "f.py:f", "tags": ["a"], "schedules": []}
        result = normalize_deployment(dep)
        assert result == {
            "name": "x",
            "entrypoint": "f.py:f",
            "tags": ["a"],
            "schedules": [],
        }

    def test_empty_dict_stays_empty(self):
        from prefect_deployments_toolkit.deployment_loader import normalize_deployment

        assert normalize_deployment({}) == {}

    def test_does_not_mutate_original(self):
        from prefect_deployments_toolkit.deployment_loader import normalize_deployment

        dep = {"name": "x", "pull": []}
        original = dep.copy()
        normalize_deployment(dep)
        assert dep == original


# ---------------------------------------------------------------------------
# get_deployments_from_source — local path
# ---------------------------------------------------------------------------


class TestGetDeploymentsFromSourceLocal:
    """Tests for source == 'local' (filesystem-based loading)."""

    def test_loads_single_deployment_from_local(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "prefect_base.yaml").write_text(BASE_YAML)
        (dep_dir / "flow_a.yaml").write_text(DEP_A_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert "flow-a" in result
        assert result["flow-a"]["entrypoint"] == "flows/a.py:flow_a"

    def test_pull_key_is_stripped_from_normalized_deployment(
        self, tmp_path, monkeypatch
    ):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "prefect_base.yaml").write_text(BASE_YAML)
        (dep_dir / "flow_a.yaml").write_text(DEP_A_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert "pull" not in result["flow-a"]

    def test_loads_multiple_deployments_from_multiple_files(
        self, tmp_path, monkeypatch
    ):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "prefect_base.yaml").write_text(BASE_YAML)
        (dep_dir / "flow_a.yaml").write_text(DEP_A_YAML)
        (dep_dir / "flow_b.yaml").write_text(DEP_B_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert set(result.keys()) == {"flow-a", "flow-b"}

    def test_returns_empty_dict_when_deployments_dir_missing(
        self, tmp_path, monkeypatch
    ):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)

        result = get_deployments_from_source("local", "test", "deployments")

        assert result == {}

    def test_base_yaml_missing_is_tolerated(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "flow_a.yaml").write_text(DEP_A_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert "flow-a" in result

    def test_prefect_base_yaml_excluded_from_deployment_scan(
        self, tmp_path, monkeypatch
    ):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "prefect_base.yaml").write_text(BASE_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert result == {}

    def test_list_format_yaml_is_parsed(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "flow_l.yaml").write_text(LIST_FORMAT_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert "flow-list" in result

    def test_yaml_without_deployments_key_is_skipped(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "irrelevant.yaml").write_text(NO_DEPLOYMENTS_KEY_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        assert result == {}

    def test_invalid_yaml_calls_sys_exit(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "bad.yaml").write_text(INVALID_YAML)

        with pytest.raises(SystemExit):
            get_deployments_from_source("local", "test", "deployments")

    def test_missing_name_field_calls_sys_exit(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "bad.yaml").write_text(MISSING_NAME_YAML)

        with pytest.raises(SystemExit):
            get_deployments_from_source("local", "test", "deployments")

    def test_non_dict_entry_calls_sys_exit(self, tmp_path, monkeypatch):
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "bad.yaml").write_text(NON_DICT_ENTRY_YAML)

        with pytest.raises(SystemExit):
            get_deployments_from_source("local", "test", "deployments")

    def test_base_yaml_merged_into_deployment_content(self, tmp_path, monkeypatch):
        """pull steps from base should appear in merged parse (though stripped from result)."""
        from prefect_deployments_toolkit.deployment_loader import (
            get_deployments_from_source,
        )

        monkeypatch.chdir(tmp_path)
        dep_dir = tmp_path / "deployments"
        dep_dir.mkdir()
        (dep_dir / "prefect_base.yaml").write_text(BASE_YAML)
        (dep_dir / "flow_a.yaml").write_text(DEP_A_YAML)

        result = get_deployments_from_source("local", "test", "deployments")

        # Deployment should be loaded (base + dep merged successfully)
        assert "flow-a" in result


# ---------------------------------------------------------------------------
# get_deployments_from_source — git source (subprocess mocked)
# ---------------------------------------------------------------------------


class TestGetDeploymentsFromSourceGit:
    """Tests for git-ref sources — subprocess.check_output is mocked."""

    def _mock_check_output(self, base_content: str, tar_files: dict[str, str]):
        """Return a side_effect function for subprocess.check_output."""
        tar_bytes = _make_tar(tar_files)
        call_count = {"n": 0}

        def _side_effect(cmd, **kwargs):
            call_count["n"] += 1
            if "show" in cmd:
                return base_content.encode()
            if "archive" in cmd:
                return tar_bytes
            raise subprocess.CalledProcessError(1, cmd)

        return _side_effect

    def test_loads_deployment_from_git_ref(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        side_effect = self._mock_check_output(
            BASE_YAML,
            {"deployments/flow_a.yaml": DEP_A_YAML},
        )
        with patch.object(dl.subprocess, "check_output", side_effect=side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert "flow-a" in result

    def test_returns_empty_when_no_deployments_dir_in_git(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return BASE_YAML.encode()
            raise subprocess.CalledProcessError(1, cmd)

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert result == {}

    def test_git_base_yaml_not_found_uses_empty_base(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        tar_bytes = _make_tar({"deployments/flow_b.yaml": DEP_B_YAML})

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                raise subprocess.CalledProcessError(1, cmd)
            if "archive" in cmd:
                return tar_bytes
            raise subprocess.CalledProcessError(1, cmd)

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert "flow-b" in result

    def test_prefect_base_yaml_excluded_from_git_tar(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        # tar contains only prefect_base.yaml — should yield no deployments
        tar_bytes = _make_tar({"deployments/prefect_base.yaml": BASE_YAML})

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return BASE_YAML.encode()
            return tar_bytes

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert result == {}

    def test_multiple_deployments_in_single_git_file(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        multi_yaml = DEP_A_YAML + DEP_B_YAML.replace("deployments:", "")
        # Actually build proper multi-deployment yaml
        multi_yaml = textwrap.dedent("""\
            deployments:
              - name: flow-a
                entrypoint: flows/a.py:flow_a
              - name: flow-b
                entrypoint: flows/b.py:flow_b
        """)
        tar_bytes = _make_tar({"deployments/multi.yaml": multi_yaml})

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return b""
            return tar_bytes

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "HEAD~1", "prev commit", "deployments"
            )

        assert set(result.keys()) == {"flow-a", "flow-b"}

    def test_non_yaml_files_in_tar_are_ignored(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        tar_bytes = _make_tar(
            {
                "deployments/flow_a.yaml": DEP_A_YAML,
                "deployments/README.md": "# docs",
            }
        )

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return b""
            return tar_bytes

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert "flow-a" in result
        assert len(result) == 1

    def test_invalid_yaml_in_git_calls_sys_exit(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        tar_bytes = _make_tar({"deployments/bad.yaml": INVALID_YAML})

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return b""
            return tar_bytes

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            with pytest.raises(SystemExit):
                dl.get_deployments_from_source(
                    "origin/main", "base branch", "deployments"
                )

    def test_pull_key_stripped_in_git_source(self):
        from prefect_deployments_toolkit import deployment_loader as dl

        tar_bytes = _make_tar({"deployments/flow_a.yaml": DEP_A_YAML})

        def _side_effect(cmd, **kwargs):
            if "show" in cmd:
                return BASE_YAML.encode()
            return tar_bytes

        with patch.object(dl.subprocess, "check_output", side_effect=_side_effect):
            result = dl.get_deployments_from_source(
                "origin/main", "base branch", "deployments"
            )

        assert "pull" not in result.get("flow-a", {})
