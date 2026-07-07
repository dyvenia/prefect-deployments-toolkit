"""Retrieve all deployments added, modified, or removed in the current branch."""

import argparse
import logging
import os
import subprocess
import sys

from .deployment_loader import get_deployments_from_source

logger = logging.getLogger(__name__)


def get_base_branch_deployments(
    base_branch: str,
    deployments_dir: str = "deployments",
) -> dict[str, dict]:
    """Get all deployments defined in the base branch."""
    try:
        subprocess.run(
            ["git", "fetch", "origin", base_branch],  # noqa: S607
            check=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        logger.warning("Failed to fetch base branch '%s'", base_branch)
    return get_deployments_from_source(
        f"origin/{base_branch}",
        f"the base branch '{base_branch}'",
        deployments_dir,
    )


def get_pr_branch_deployments(deployments_dir: str = "deployments") -> dict[str, dict]:
    """Get all deployments defined in the current branch (local filesystem)."""
    return get_deployments_from_source("local", "the current branch", deployments_dir)


def get_previous_commit_deployments(
    deployments_dir: str = "deployments",
) -> dict[str, dict]:
    """Get all deployments defined in the previous commit (HEAD~1)."""
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD~1"],
        capture_output=True,
    )
    if result.returncode != 0:
        logger.warning(
            "HEAD~1 does not exist (possibly initial commit). Returning empty."
        )
        return {}
    return get_deployments_from_source("HEAD~1", "the previous commit", deployments_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--modified-by",
        choices=["pull_request_target", "pull_request", "push"],
        help="GitHub Actions event type driving this run.",
    )
    parser.add_argument(
        "--base-ref",
        required=True,
        help="Base branch name (e.g. master, main, rc_3.3.1).",
    )
    parser.add_argument(
        "--deployments-dir",
        default="deployments",
        help="Path to the deployments directory (default: deployments).",
    )
    args = parser.parse_args()

    is_commit_compare = args.modified_by in ("pull_request_target", "push")

    previous = (
        get_previous_commit_deployments(args.deployments_dir)
        if is_commit_compare
        else get_base_branch_deployments(args.base_ref, args.deployments_dir)
    )
    current = (
        get_base_branch_deployments(args.base_ref, args.deployments_dir)
        if is_commit_compare
        else get_pr_branch_deployments(args.deployments_dir)
    )

    new_or_modified = [
        name
        for name, cfg in current.items()
        if name not in previous or cfg != previous[name]
    ]
    removed = [name for name in previous if name not in current]

    if not new_or_modified and not removed:
        logger.info("No new, modified, or removed deployments found.")
        sys.exit(0)

    all_changed = ",".join(new_or_modified + removed)
    logger.info(
        "Found the following new, modified, or removed deployments: '%s'.", all_changed
    )
    logger.info("New or modified: '%s'.", ",".join(new_or_modified))
    logger.info("Removed: '%s'.", ",".join(removed))
    print(all_changed)

    github_env = os.getenv("GITHUB_ENV")
    if os.getenv("GITHUB_ACTION") and github_env:
        with open(github_env, "a") as f:
            f.write(f"DEPLOYMENT_NAMES={all_changed}\n")
            f.write(f"NEW_OR_MODIFIED_DEPLOYMENT_NAMES={','.join(new_or_modified)}\n")
            f.write(f"REMOVED_DEPLOYMENT_NAMES={','.join(removed)}\n")


if __name__ == "__main__":
    main()
