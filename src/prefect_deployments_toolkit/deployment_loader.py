"""Load and normalize Prefect deployment configs from local filesystem or git refs."""

import json
import logging
import subprocess
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Keys excluded from deployment comparison — they don't affect runtime behaviour
_IGNORED_KEYS = frozenset({"pull"})


def normalize_deployment(deployment: dict) -> dict:
    """Return deployment config with non-comparable keys removed."""
    return {k: v for k, v in deployment.items() if k not in _IGNORED_KEYS}


def get_deployments_from_source(
    source: str,
    source_name: str,
    deployments_dir: str = "deployments",
) -> dict[str, dict]:
    """Load all deployments from a git ref or the local filesystem.

    Parameters
    ----------
    source:
        Git reference (e.g. "origin/main", "HEAD~1") or "local" for the
        working-tree filesystem.
    source_name:
        Human-readable label used in log messages.
    deployments_dir:
        Path to the deployments directory.

    Returns
    -------
    dict mapping deployment name → normalised config dict.
    """
    logger.info("Getting deployments in %s...", source_name)
    deployments: dict[str, dict] = {}
    is_local = source == "local"
    base_file_path = f"{deployments_dir}/prefect_base.yaml"

    try:
        # --- load prefect_base.yaml ---
        try:
            if is_local:
                base_path = Path(base_file_path)
                base_content = base_path.read_text() if base_path.exists() else ""
            else:
                base_content = subprocess.check_output(
                    ["git", "show", f"{source}:{base_file_path}"],  # noqa: S607
                ).decode()
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("prefect_base.yaml not found in %s", source_name)
            base_content = ""

        # --- enumerate deployment YAML files ---
        if is_local:
            deployment_dir = Path(deployments_dir)
            if not deployment_dir.exists():
                logger.info("No %s folder found in %s", deployments_dir, source_name)
                return deployments
            yaml_files = [
                str(f.relative_to("."))
                for f in deployment_dir.rglob("*.yaml")
                if f.name != "prefect_base.yaml"
            ]
        else:
            raw = subprocess.check_output(
                ["git", "ls-tree", "-r", "--name-only", source, f"{deployments_dir}/"],  # noqa: S607
                stderr=subprocess.DEVNULL,
            ).decode().strip().split("\n")
            yaml_files = [
                f for f in raw
                if f
                and f.endswith(".yaml")
                and f.startswith(f"{deployments_dir}/")
                and "prefect_base.yaml" not in f
            ]

        logger.info("Found %d YAML files in %s folder", len(yaml_files), deployments_dir)

        # --- parse each file ---
        for yaml_file in yaml_files:
            try:
                if is_local:
                    deployment_content = Path(yaml_file).read_text()
                else:
                    deployment_content = subprocess.check_output(
                        ["git", "show", f"{source}:{yaml_file}"],  # noqa: S607
                    ).decode()

                parsed = yaml.safe_load(base_content + "\n" + deployment_content)

                if isinstance(parsed, dict) and "deployments" in parsed:
                    deployment_list = parsed["deployments"]
                elif isinstance(parsed, list):
                    deployment_list = parsed
                else:
                    deployment_list = None

                if not isinstance(deployment_list, list):
                    continue

                for d in deployment_list:
                    if not isinstance(d, dict):
                        logger.error(
                            "Invalid deployment entry in %s: expected dict, got %s: %s",
                            yaml_file, type(d).__name__, d,
                        )
                        sys.exit(1)
                    name = d.get("name")
                    if not isinstance(name, str) or not name:
                        logger.error(
                            "Deployment in %s is missing a valid 'name' field: %s",
                            yaml_file, d,
                        )
                        sys.exit(1)
                    deployments[name] = normalize_deployment(d)

                logger.debug("Loaded %d deployments from %s", len(deployment_list), yaml_file)

            except yaml.YAMLError as exc:
                logger.error("Invalid YAML syntax in %s: %s", yaml_file, exc)
                sys.exit(1)
            except (subprocess.CalledProcessError, OSError) as exc:
                logger.warning("Failed to load deployments from %s: %s", yaml_file, exc)
                continue

    except subprocess.CalledProcessError:
        logger.info("No %s folder found in %s", deployments_dir, source_name)

    logger.debug("Deployments in %s:\n%s", source_name, json.dumps(deployments, indent=2))
    return deployments