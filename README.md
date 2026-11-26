# Prefect Deployments Toolkit

![PyPI](https://img.shields.io/pypi/v/prefect-deployments-toolkit?color=blue&style=flat-square)
[![PyPI Downloads](https://static.pepy.tech/badge/prefect-deployments-toolkit/month)](https://pepy.tech/projects/prefect-deployments-toolkit)

A lightweight CLI toolkit for managing Prefect deployment lifecycles in CI/CD pipelines. It detects deployments that were added, modified, or removed between two git references, and applies (creates, updates, or deletes) those deployments against Prefect Cloud or a self-hosted Prefect server — via either the Prefect CLI or direct REST API calls.

Built for teams running many Prefect flows out of a monorepo, where each deployment is defined in its own YAML file alongside a shared `prefect_base.yaml`, and CI needs to figure out _which_ deployments actually changed before re-registering them.

## Features

- **Change detection** — diff deployments between two git refs (a merge request source vs. target branch, or `HEAD~1` vs. `HEAD` after a merge) and get back exactly which deployments were added, modified, or removed.
- **Unified push / pull-request logic** — one code path handles both `pull_request` (compare local files against a fetched base branch) and `push`/`pull_request_target` (compare `HEAD~1` against `HEAD`) event styles.
- **Dual deploy backends** — apply deployments using the `prefect` CLI as a subprocess, or bypass it entirely with direct REST API calls for faster, more controllable deploys. Both backends work against Prefect Cloud and self-hosted/OSS Prefect servers.
- **Environment overrides** — optional deployment name prefixing (`--deployment-name-prefix`), work pool substitution (`--work-pool`), custom container images (`--custom-image`), and schedule enable/pause control (`--enable-schedule`).
- **Flow-scoped parameter overrides** — override parameters for deployments using a specific entrypoint function with repeatable `--param FUNCTION:PARAMETER=VALUE` arguments.
- **Branch configuration** — configure the Git reference (`--reference`), repository name (`--repo-name`), and optional dbt models directory (`--models-dir`) used for branch-specific job variable paths.
- **Deployment selection** — select deployments by name (`--deployment-names`) and configure the deployment definitions directory (`--deployments-dir`).
- **Configurable tags** — set the environment tag (`--tag`), append work queue names (`--add-work-queue-tag`) and directory components (`--add-path-tags`), and optionally format generated tags as key-value pairs (`--key-value-tags`).
- **Concurrent deployment application** — apply multiple deployments in parallel with clean, non-interleaved log output per deployment and configurable concurrency (`--max-workers`).
- **Duplicate deployment name detection** — warns or optionally cleans up when the same deployment name resolves to more than one flow (`--enforce-unique-deployment-names`).

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
  --work-pool my-dev-work-pool \
  --deployment-name-prefix dev-- \
  --backend rest \
  --enforce-unique-deployment-names false \
  --max-workers 8 \
  --models-dir models \
  --add-work-queue-tag true \
  --add-path-tags true \
  --key-value-tags true \
  --param "transform_and_catalog:dbt_repo_branch=feature-branch-1"
```

| Argument                            | Required | Default       | Description                                                                                                                           |
| ----------------------------------- | -------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `--deployment-names`                | Yes      | —             | Comma-separated deployment names to apply or remove.                                                                                  |
| `--enable-schedule`                 | Yes      | —             | Enable deployment schedules: `true` or `false`.                                                                                       |
| `--tag`                             | Yes      | —             | Environment or image tag, such as `dev` or `prd`.                                                                                     |
| `--reference`                       | Yes      | —             | Git branch or tag reference used for registration.                                                                                    |
| `--repo-name`                       | Yes      | —             | Repository name used in branch-specific job variable paths.                                                                           |
| `--custom-image`                    | No       | Empty string  | Container image override. Takes precedence over the image configured in YAML.                                                         |
| `--deployments-dir`                 | No       | `deployments` | Directory containing deployment definitions and `prefect_base.yaml`.                                                                  |
| `--work-pool`                       | No       | Empty string  | Override the work pool configured in YAML.                                                                                            |
| `--deployment-name-prefix`          | No       | Empty string  | Prefix added to deployment names, such as `dev--`.                                                                                    |
| `--backend`                         | No       | `cli`         | Deployment backend: `cli` or `rest`.                                                                                                  |
| `--enforce-unique-deployment-names` | No       | `false`       | Remove duplicate deployments under other flows when `true`; otherwise, only warn.                                                     |
| `--max-workers`                     | No       | `8`           | Maximum number of deployments applied concurrently.                                                                                   |
| `--models-dir`                      | No       | Empty string  | Relative dbt models directory used to generate `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` for references other than `main` or `master`. |
| `--add-work-queue-tag`              | No       | `false`       | Append the configured work queue name to deployment tags.                                                                             |
| `--add-path-tags`                   | No       | `false`       | Append deployment directory components relative to `--deployments-dir` as tags.                                                       |
| `--key-value-tags`                  | No       | `false`       | Format generated tags as `env=...`, `ref=...`, `queue=...`, and `path=...`. YAML-defined tags remain unchanged.                       |
| `--param`                           | No       | No overrides  | Repeatable parameter override in `FUNCTION:PARAMETER=VALUE` format. Values are strings.                                               |

`--deployment-name-prefix` is optional. Pass a value such as `dev--` to prefix every deployment name. `--work-pool` overrides the work pool set in YAML for any tag.

`--param` matches the function name after `:` in `entrypoint`, not the deployment name. Only selected deployments using that function receive the override; other parameters and source YAML files remain unchanged. Repeat the argument to override multiple parameters. For duplicate function/parameter pairs, the last value wins.

Parameter overrides work with both backends. Environment and trigger restrictions, such as applying overrides only for DEV PRs or manual runs, must be configured in the calling workflow.

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

| Backend | How it works                           | When to use                                                            |
| ------- | -------------------------------------- | ---------------------------------------------------------------------- |
| `cli`   | Shells out to `prefect deploy`         | Simplest, matches local `prefect deploy` behavior exactly              |
| `rest`  | Talks directly to the Prefect REST API | Faster for many concurrent deployments, avoids CLI subprocess overhead |

Both backends work against Prefect Cloud (`api.prefect.cloud`) and self-hosted/OSS Prefect servers — the target is determined entirely by `PREFECT_API_URL`.

## Environment Variables

| Variable                  | Required                                             | Purpose                                                                                                                                                  |
| ------------------------- | ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PREFECT_API_URL`         | Yes                                                  | Full API base URL. Cloud: `https://api.prefect.cloud/api/accounts/<id>/workspaces/<id>`. Self-hosted: e.g. `https://your-prefect-server.company.com/api` |
| `PREFECT_API_KEY`         | Only for Prefect Cloud                               | Cloud API key, sent as a Bearer token                                                                                                                    |
| `PREFECT_API_AUTH_STRING` | Only for self-hosted servers with Basic auth enabled | Sent as HTTP Basic auth (base64-encoded)                                                                                                                 |

Neither `PREFECT_API_KEY` nor `PREFECT_API_AUTH_STRING` is required if your self-hosted Prefect server has no authentication configured — only `PREFECT_API_URL` is mandatory in every setup.

## License

Licensed under [PolyForm Noncommercial 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0). You may view, use, and modify this software for noncommercial purposes. Commercial use — including building products, services, or automation on top of it — requires a separate license. Contact `kwolski@dyvenia.com` for commercial licensing inquiries.

## Contributing

Issues and pull requests are welcome for noncommercial use cases. Please open an issue before submitting a large PR to discuss scope.
