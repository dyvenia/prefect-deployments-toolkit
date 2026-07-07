"""Unit tests for prefect_deployments_toolkit.yaml_utils."""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BASE_YAML = textwrap.dedent("""\
    pull:
      - prefect.deployments.steps.git_clone:
          repository: git@github.com:org/repo.git
          branch: main
          access_token: "{{ prefect.blocks.secret.gh-pat }}"
""")

DEPLOYMENT_YAML = textwrap.dedent("""\
    deployments:
      - name: my-flow
        entrypoint: flows/my_flow.py:my_flow
        work_pool:
          name: default-pool
          job_variables:
            image: myrepo/myimage:latest
            cpu: "1"
        tags:
          - etl
          - production
        schedules:
          - cron: "0 6 * * *"
            timezone: Europe/Warsaw
""")

MERGED_YAML = BASE_YAML + "\n" + DEPLOYMENT_YAML


def write_tmp(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# build_merged_prefect_file
# ---------------------------------------------------------------------------


class TestBuildMergedPrefectFile:
    def test_concatenates_base_and_deployment(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import build_merged_prefect_file

        base = write_tmp(tmp_path, "base.yaml", BASE_YAML)
        dep = write_tmp(tmp_path, "dep.yaml", DEPLOYMENT_YAML)
        dest = tmp_path / "merged.yaml"
        build_merged_prefect_file(base, dep, dest)
        result = dest.read_text()
        assert "git_clone" in result
        assert "my-flow" in result

    def test_dest_contains_both_sections(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import build_merged_prefect_file

        base = write_tmp(tmp_path, "base.yaml", BASE_YAML)
        dep = write_tmp(tmp_path, "dep.yaml", DEPLOYMENT_YAML)
        dest = tmp_path / "merged.yaml"
        build_merged_prefect_file(base, dep, dest)
        parsed = yaml.safe_load(dest.read_text())
        assert "pull" in parsed
        assert "deployments" in parsed


# ---------------------------------------------------------------------------
# load_deployment_config
# ---------------------------------------------------------------------------


class TestLoadDeploymentConfig:
    def test_returns_correct_deployment(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import load_deployment_config

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        config = load_deployment_config(merged, "my-flow")
        assert config["name"] == "my-flow"
        assert config["entrypoint"] == "flows/my_flow.py:my_flow"

    def test_raises_on_missing_deployment(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import load_deployment_config

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        with pytest.raises(KeyError, match="nonexistent"):
            load_deployment_config(merged, "nonexistent")


# ---------------------------------------------------------------------------
# validate_schedule
# ---------------------------------------------------------------------------


class TestValidateSchedule:
    def test_valid_cron_schedule_passes(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import validate_schedule

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        # Should not raise
        validate_schedule(merged, "my-flow")

    def test_singular_schedule_key_raises(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import validate_schedule

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: bad-schedule
                entrypoint: flows/f.py:f
                schedule:
                  cron: "0 * * * *"
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        with pytest.raises(ValueError, match="singular"):
            validate_schedule(merged, "bad-schedule")

    def test_empty_schedules_list_raises(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import validate_schedule

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: empty-sched
                entrypoint: flows/f.py:f
                schedules: []
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        with pytest.raises(ValueError, match="empty"):
            validate_schedule(merged, "empty-sched")

    def test_schedule_missing_cron_raises(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import validate_schedule

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: no-cron
                entrypoint: flows/f.py:f
                schedules:
                  - timezone: UTC
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        with pytest.raises(ValueError, match="cron"):
            validate_schedule(merged, "no-cron")

    def test_no_schedules_key_is_valid(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import validate_schedule

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: no-sched
                entrypoint: flows/f.py:f
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        # Should not raise
        validate_schedule(merged, "no-sched")


# ---------------------------------------------------------------------------
# apply_dev_overrides
# ---------------------------------------------------------------------------


class TestApplyDevOverrides:
    def test_prefixes_deployment_name(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import apply_dev_overrides

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        apply_dev_overrides(merged, "my-flow", "dev--", "dev-pool")
        parsed = yaml.safe_load(merged.read_text())
        names = [d["name"] for d in parsed["deployments"]]
        assert "dev--my-flow" in names
        assert "my-flow" not in names

    def test_sets_dev_work_pool(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import apply_dev_overrides

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        apply_dev_overrides(merged, "my-flow", "dev--", "my-dev-pool")
        parsed = yaml.safe_load(merged.read_text())
        dep = next(d for d in parsed["deployments"] if d["name"] == "dev--my-flow")
        assert dep["work_pool"]["name"] == "my-dev-pool"

    def test_unknown_deployment_name_is_noop(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import apply_dev_overrides

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        apply_dev_overrides(merged, "unknown-dep", "dev--", "dev-pool")
        # File is rewritten but deployment names should be unchanged
        parsed = yaml.safe_load(merged.read_text())
        names = [d["name"] for d in parsed["deployments"]]
        assert names == ["my-flow"]


# ---------------------------------------------------------------------------
# set_git_clone_branch
# ---------------------------------------------------------------------------


class TestSetGitCloneBranch:
    def test_sets_branch_in_pull_step(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import set_git_clone_branch

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        set_git_clone_branch(merged, "feature-xyz")
        parsed = yaml.safe_load(merged.read_text())
        for step in parsed.get("pull", []):
            gc = step.get("prefect.deployments.steps.git_clone")
            if gc:
                assert gc["branch"] == "feature-xyz"
                return
        pytest.fail("git_clone step not found in pull")

    def test_no_pull_steps_does_not_crash(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import set_git_clone_branch

        content = DEPLOYMENT_YAML  # no pull section
        merged = write_tmp(tmp_path, "merged.yaml", content)
        set_git_clone_branch(merged, "some-branch")
        # Should complete without exception


# ---------------------------------------------------------------------------
# set_schedules_active
# ---------------------------------------------------------------------------


class TestSetSchedulesActive:
    def test_sets_all_schedules_active_true(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import set_schedules_active

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        set_schedules_active(merged, "my-flow", active=True)
        parsed = yaml.safe_load(merged.read_text())
        dep = next(d for d in parsed["deployments"] if d["name"] == "my-flow")
        assert all(s["active"] is True for s in dep.get("schedules", []))

    def test_sets_all_schedules_active_false(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import set_schedules_active

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: multi-sched
                entrypoint: flows/f.py:f
                schedules:
                  - cron: "0 6 * * *"
                    active: true
                  - cron: "0 18 * * *"
                    active: true
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        set_schedules_active(merged, "multi-sched", active=False)
        parsed = yaml.safe_load(merged.read_text())
        dep = next(d for d in parsed["deployments"] if d["name"] == "multi-sched")
        assert all(s["active"] is False for s in dep.get("schedules", []))


# ---------------------------------------------------------------------------
# get_deployment_tags
# ---------------------------------------------------------------------------


class TestGetDeploymentTags:
    def test_returns_tags_list(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import get_deployment_tags

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        tags = get_deployment_tags(merged, "my-flow")
        assert "etl" in tags
        assert "production" in tags

    def test_returns_empty_list_when_no_tags(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import get_deployment_tags

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: no-tags
                entrypoint: flows/f.py:f
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        tags = get_deployment_tags(merged, "no-tags")
        assert tags == []


# ---------------------------------------------------------------------------
# get_job_variables
# ---------------------------------------------------------------------------


class TestGetJobVariables:
    def test_returns_job_variables(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import get_job_variables

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        jv = get_job_variables(merged, "my-flow")
        assert jv["image"] == "myrepo/myimage:latest"
        assert jv["cpu"] == "1"

    def test_returns_empty_dict_when_no_job_variables(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import get_job_variables

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: no-jv
                entrypoint: flows/f.py:f
                work_pool:
                  name: some-pool
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        jv = get_job_variables(merged, "no-jv")
        assert jv == {}


# ---------------------------------------------------------------------------
# has_schedules
# ---------------------------------------------------------------------------


class TestHasSchedules:
    def test_true_when_schedules_present(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import has_schedules

        merged = write_tmp(tmp_path, "merged.yaml", MERGED_YAML)
        assert has_schedules(merged, "my-flow") is True

    def test_false_when_no_schedules_key(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import has_schedules

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: no-sched
                entrypoint: flows/f.py:f
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        assert has_schedules(merged, "no-sched") is False

    def test_false_when_schedules_is_null(self, tmp_path):
        from prefect_deployments_toolkit.yaml_utils import has_schedules

        content = BASE_YAML + textwrap.dedent("""\
            deployments:
              - name: null-sched
                entrypoint: flows/f.py:f
                schedules: ~
        """)
        merged = write_tmp(tmp_path, "merged.yaml", content)
        assert has_schedules(merged, "null-sched") is False


# ---------------------------------------------------------------------------
# find_deployment_file  (subprocess.run is mocked)
# ---------------------------------------------------------------------------


class TestFindDeploymentFile:
    """find_deployment_file shells out to grep — mock subprocess.run."""

    def _mock_grep(self, stdout: str):
        m = MagicMock()
        m.stdout = stdout
        return m

    def test_returns_path_for_single_match(self, tmp_path):
        from prefect_deployments_toolkit import yaml_utils

        file_path = tmp_path / "dep.yaml"
        file_path.touch()
        grep_out = f"{file_path}:  - name: my-flow\n"
        with patch.object(
            yaml_utils.subprocess, "run", return_value=self._mock_grep(grep_out)
        ):
            result = yaml_utils.find_deployment_file("my-flow", tmp_path)
        assert result == file_path

    def test_returns_none_when_no_match(self, tmp_path):
        from prefect_deployments_toolkit import yaml_utils

        with patch.object(
            yaml_utils.subprocess, "run", return_value=self._mock_grep("")
        ):
            result = yaml_utils.find_deployment_file("ghost", tmp_path)
        assert result is None

    def test_raises_on_multiple_file_matches(self, tmp_path):
        from prefect_deployments_toolkit import yaml_utils

        f1 = tmp_path / "a.yaml"
        f2 = tmp_path / "b.yaml"
        f1.touch()
        f2.touch()
        grep_out = f"{f1}:  - name: dup\n{f2}:  - name: dup\n"
        with patch.object(
            yaml_utils.subprocess, "run", return_value=self._mock_grep(grep_out)
        ):
            with pytest.raises(ValueError, match="multiple files"):
                yaml_utils.find_deployment_file("dup", tmp_path)
