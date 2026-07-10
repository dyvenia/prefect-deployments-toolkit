# Prefect Deployments Toolkit

![PyPI](https://img.shields.io/pypi/v/prefect-deployments-toolkit?color=blue&style=flat-square)
[![PyPI Downloads](https://static.pepy.tech/badge/prefect-deployments-toolkit/month)](https://pepy.tech/projects/prefect-deployments-toolkit)

A lightweight CLI toolkit for managing Prefect deployment lifecycles in CI/CD pipelines. It detects deployments that were added, modified, or removed between two git references, and applies (creates, updates, or deletes) those deployments against Prefect Cloud or a self-hosted Prefect server — via either the Prefect CLI or direct REST API calls.

Built for teams running many Prefect flows out of a monorepo, where each deployment is defined in its own YAML file alongside a shared `prefect_base.yaml`, and CI needs to figure out _which_ deployments actually changed before re-registering them.

## Features

- **Change detection** — diff deployments between two git refs (a merge request source vs. target branch, or `HEAD~1` vs. `HEAD` after a merge) and get back exactly which deployments were added, modified, or removed.
- **Unified push / pull-request logic** — one code path handles both `pull_request` (compare local files against a fetched base branch) and `push`/`pull_request_target` (compare `HEAD~1` against `HEAD`) event styles.
- **Dual deploy backends** — apply deployments using the `prefect` CLI as a subprocess, or bypass it entirely with direct Prefect Cloud REST API calls for faster, more controllable deploys.
- **Dev/prod environment overrides** — automatic name prefixing, work pool substitution, and schedule pausing for dev environments.
- **Concurrent deployment application** — apply multiple deployments in parallel with clean, non-interleaved log output per deployment.
- **Duplicate deployment name detection** — warns (or optionally cleans up) when the same deployment name resolves to more than one flow.

## Installation

```bash
pip install prefect-deployments-toolkit
```

Requires Python >= 3.9.

## Usage

### 1. Detect modified deployments

```bash
python -m prefect_deployments_toolkit.get_modified_deployments \
  --modified-by push \
  --base-ref main \
  --deployments-dir deployments
```

Prints a comma-separated list of changed deployment names to stdout, and (if running inside GitHub Actions) writes `DEPLOYMENT_NAMES`, `NEW_OR_MODIFIED_DEPLOYMENT_NAMES`, and `REMOVED_DEPLOYMENT_NAMES` to `GITHUB_ENV`.

`--modified-by` accepts:

- `pull_request` — compares your local working tree against the fetched base branch (use during an open MR/PR).
- `push` or `pull_request_target` — compares `HEAD~1` against `HEAD` (use after a merge to a protected branch).

### 2. Apply deployments

```bash
python -m prefect_deployments_toolkit \
  --deployment-names "flow_a,flow_b" \
  --enable-schedule false \
  --tag dev \
  --reference feature-branch-1 \
  --repo-name my-flows-repo \
  --custom-image "" \
  --deployments-dir deployments \
  --dev-work-pool my-dev-work-pool \
  --backend rest \
  --enforce-unique-deployment-names false
```

## Expected Repository Layout

```bash
deployments/
├── prefect_base.yaml # shared config merged into every deployment
├── flow_a.yaml
├── flow_b.yaml
└── ...
```

Each deployment YAML file must declare a `deployments:` list with at least a `name` and `entrypoint` key, following the standard Prefect deployment YAML schema.

## Backends

| Backend | How it works                                 | When to use                                                            |
| ------- | -------------------------------------------- | ---------------------------------------------------------------------- |
| `cli`   | Shells out to `prefect deploy`               | Simplest, matches local `prefect deploy` behavior exactly              |
| `rest`  | Talks directly to the Prefect Cloud REST API | Faster for many concurrent deployments, avoids CLI subprocess overhead |

## Environment Variables

| Variable          | Required | Purpose                                |
| ----------------- | -------- | -------------------------------------- |
| `PREFECT_API_URL` | Yes      | Full Prefect Cloud/server API base URL |
| `PREFECT_API_KEY` | Yes      | API key for authentication             |

## License

Licensed under [PolyForm Noncommercial 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0). You may view, use, and modify this software for noncommercial purposes. Commercial use — including building products, services, or automation on top of it — requires a separate license. Contact `kwolski@dyvenia.com` for commercial licensing inquiries.

## Contributing

Issues and pull requests are welcome for noncommercial use cases. Please open an issue before submitting a large PR to discuss scope.
