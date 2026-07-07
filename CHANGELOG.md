# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.7] - 2026-07-07

### Added

- work_queue_name added to the payload for rest api
- description, version, concurrency_limit, concurrency_options added to the payload for rest api

[0.0.7]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.7

## [0.0.6] - 2026-07-06

### Added

- parameter_openapi_schema passed during prefect deployment registration via rest api
- resolve variables functionality added
- prefect_api merged into prefect_rest
- logs refinement - avoid duplicated get_flow_ids_for_deployment

[0.0.6]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.6

## [0.0.5] - 2026-07-06

### Added

- module import fix

[0.0.5]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.5

## [0.0.4] - 2026-07-06

### Added

- prefect OSS support
- get_modified_deployments performance improvements - quicker yaml load with CSafeLoader fix

[0.0.4]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.4

## [0.0.3] - 2026-07-06

### Added

- get_modified_deployments performance improvements - quicker yaml load with CSafeLoader
- get_modified_deployments performance improvements - single subprocess for all deployments
- serializing entrypoint imports process-wide to avoid deadlocks

[0.0.3]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.3

## [0.0.2] - 2026-07-04

### Added

- Logging adjustments

[0.0.2]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.2

## [0.0.1] - 2026-07-03

### Added

- Initial release of the toolkit.
- `get_modified_deployments` module to detect added, modified, and removed Prefect deployments between two git references.
- Unified handling of `push`, `pull_request`, and `pull_request_target` CI event types for change detection.
- `apply_deployments` module (via `__main__`) to create, update, or delete Prefect deployments concurrently.
- Dual backend support: `cli` (wraps `prefect deploy`) and `rest` (direct Prefect Cloud REST API calls).
- Dev environment overrides: deployment name prefixing, work pool substitution, and schedule pausing.
- Duplicate deployment name detection with optional automatic cleanup (`--enforce-unique-deployment-names`).
- Thread-safe, non-interleaved log buffering for concurrent deployment runs.
- YAML schedule validation (rejects singular `schedule` key, empty `schedules`, and entries missing `cron`).

[0.0.1]: https://github.com/yourusername/prefect-deployments-toolkit/releases/tag/v0.0.1

