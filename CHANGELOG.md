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

[Unreleased]: https://github.com/masthead-data/airflow-reservations/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/masthead-data/airflow-reservations/releases/tag/v0.2.0
[0.1.0]: https://github.com/masthead-data/airflow-reservations/releases/tag/v0.1.0
