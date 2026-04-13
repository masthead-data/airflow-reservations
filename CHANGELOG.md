<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.2.2] - 2026-04-13

### Added

- `SECURITY.md` with vulnerability reporting process and response SLA.
- `py.typed` PEP 561 marker; package now declares itself fully typed.
- CycloneDX SBOM generated at release time and attached to every GitHub Release.
- Sigstore PEP 740 attestations published alongside PyPI artifacts for cryptographic build provenance.

### Security

- All GitHub Actions pinned to immutable commit SHAs to prevent tag-hijacking attacks.
- `pip-audit` added as a required CI step; every push is scanned against known CVE databases.

## [0.2.1] - 2026-03-31

### Fixed

- Task IDs are normalized by replacing dots with hyphens for BigQuery label compatibility when TaskGroups are used.

## [0.2.0] - 2026-02-09

### Added

- One-time debug info initializer to log versions for Airflow, Google providers, and BigQuery clients.

### Changed

- Directly sets the `reservation` field in `BigQueryInsertJobOperator` and `api_resource_configs` in `BigQueryExecuteQueryOperator`.
- Adjusted provider package version requirement for `apache-airflow-providers-google` to `10.26.0` in Airflow v2 tests.

### Deprecated

- SQL string prepending for reservation handling in favor of direct attribute injection.
- BigQuery Check Operators are not available for reservation management.

## [0.1.0] - 2026-01-22

### Added

- Initial release of `airflow-reservations` package.

[Unreleased]: https://github.com/masthead-data/airflow-reservations/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/masthead-data/airflow-reservations/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/masthead-data/airflow-reservations/releases/tag/v0.2.1
[0.2.0]: https://github.com/masthead-data/airflow-reservations/releases/tag/v0.2.0
[0.1.0]: https://github.com/masthead-data/airflow-reservations/releases/tag/v0.1.0
