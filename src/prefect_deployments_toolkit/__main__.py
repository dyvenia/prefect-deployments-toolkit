"""Entry point: parse args and apply all deployments concurrently.

Usage:
python -m apply_deployments \
    --deployment-names "hello_world3,hello_world_4" \
    --enable-schedule false \
    --tag dev \
    --reference feature_branch1 \
    --repo-name edp-flows \
    --custom-image "" \
    --deployments-dir prefect/deployments \
    --dev-work-pool lapp-dev-work-pool-prefect3 \
    --backend cli \
    --enforce-unique-deployment-names false
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .deployment import DeploymentContext, apply_single_deployment
from .log_buffer import buffered_deployment_log

logger = logging.getLogger(__name__)

MAX_WORKERS = 8


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Prefect deployments.")
    parser.add_argument("--deployment-names", required=True,
                         help="Comma-separated list of deployment names.")
    parser.add_argument("--enable-schedule", required=True,
                         choices=["true", "false"],
                         help="Whether to enable schedules after deploy.")
    parser.add_argument("--tag", required=True,
                         help="Image/environment tag (e.g. 'dev', 'v1.2.3').")
    parser.add_argument("--reference", required=True,
                         help="Git branch or tag reference (e.g. 'main', 'feature_branch1').")
    parser.add_argument("--repo-name", required=True,
                         help="Repository name used in job variable paths.")
    parser.add_argument("--custom-image", default="",
                         help="Full custom image reference, or empty string to use pool default.")
    parser.add_argument("--deployments-dir", default="deployments",
                         help="Path to the deployments directory.")
    parser.add_argument("--dev-work-pool", default="",
                         help="Dev work pool name. When set with --tag=dev, activates dev overrides.")
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS,
                         help=f"Max concurrent deployments (default: {MAX_WORKERS}).")
    parser.add_argument("--backend", default="cli", choices=["cli", "rest"],
                         help="Deployment backend: 'cli' (prefect CLI, default) or "
                              "'rest' (direct Prefect Cloud REST API calls).")
    parser.add_argument("--enforce-unique-deployment-names", default="false",
                        choices=["true", "false"],
                        help=(
                            "If false (default), only WARN in logs when a deployment name is used by"
                            "more than one flow — nothing is deleted. If true, delete every duplicate"
                            "deployment record except the one matching the flow currently resolved from"
                            "its entrypoint, enforcing globally unique deployment names."
                        ))
    return parser.parse_args()


def _run_deployment(
    deployment_name: str,
    index: int,
    total: int,
    ctx: DeploymentContext,
) -> str | None:
    """Run a single deployment inside a buffered log context.

    Returns the deployment_name if it failed, None on success.
    """
    separator = "#" * 50
    with buffered_deployment_log(deployment_name):
        logger.info("%s [%d/%d] %s %s", separator, index, total, deployment_name, separator)
        try:
            apply_single_deployment(deployment_name, ctx)
            logger.info(">>> DONE [%d/%d] %s", index, total, deployment_name)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error(">>> FAILED [%d/%d] %s: %s", index, total, deployment_name, exc)
            return deployment_name


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = _parse_args()
    names = [n.strip() for n in args.deployment_names.split(",") if n.strip()]

    if not names:
        logger.error("No deployment names provided.")
        sys.exit(1)

    ctx = DeploymentContext(
        deployment_names=names,
        enable_schedule=args.enable_schedule == "true",
        tag=args.tag,
        reference=args.reference,
        repo_name=args.repo_name,
        custom_image=args.custom_image,
        deployments_dir=Path(args.deployments_dir),
        dev_prefect_work_pool_name=args.dev_work_pool,
        backend=args.backend,
        enforce_unique_deployment_names=args.enforce_unique_deployment_names == "true",
    )

    total = len(names)
    workers = min(args.max_workers, total)
    logger.info(
        "Applying %d deployment(s) with up to %d concurrent workers (backend=%s).",
        total, workers, ctx.backend,
    )

    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_run_deployment, name, i, total, ctx): name
            for i, name in enumerate(names, start=1)
        }
        for future in as_completed(futures):
            result = future.result()  # re-raises only unexpected executor errors
            if result is not None:
                failed.append(result)

    if failed:
        logger.error(
            "%d deployment(s) failed: %s",
            len(failed),
            ", ".join(failed),
        )
        sys.exit(1)

    logger.info("All %d deployment(s) applied successfully.", total)


if __name__ == "__main__":
    main()
 