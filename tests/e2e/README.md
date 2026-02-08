# E2E Testing

End-to-end testing for the Airflow Reservations Policy plugin across multiple Airflow versions.

## Quick Start

```bash
# Test with Airflow 2.x (default)
make e2e

# Test with Airflow 3.x
make e2e-airflow3

# Test both versions
make e2e-all
```

## Architecture

The e2e test infrastructure is designed to test the plugin with both Airflow 2.x and 3.x using a unified framework:

```
tests/e2e/
├── lib/
│   └── common.sh              # Shared test utilities and functions
├── run_e2e_test.sh            # Unified test runner (supports both versions)
├── docker-compose.yml         # Airflow 2.x multi-container setup
├── docker-compose.airflow-3-standalone.yml  # Airflow 3.x standalone setup
├── dags/                      # Test DAGs and configuration
│   ├── test_reservation_dag.py
│   └── reservations_config.json
```

## Test Runner

The unified test runner (`run_e2e_test.sh`) supports multiple Airflow versions through configuration:

### Usage

```bash
# Test with specific version
./tests/e2e/run_e2e_test.sh --version 2    # Airflow 2.x
./tests/e2e/run_e2e_test.sh --version 3    # Airflow 3.x

# Keep containers running for debugging
./tests/e2e/run_e2e_test.sh --version 2 --keep

# Show full logs on failure
./tests/e2e/run_e2e_test.sh --version 3 --logs
```

### Options

- `--version, -v VERSION`: Airflow version to test (2 or 3, default: 2)
- `--keep, -k`: Keep containers running after test (useful for debugging)
- `--logs, -l`: Show full container logs on failure
- `--help, -h`: Show help message

## Airflow Version Differences

### Airflow 2.x (2.10.4-python3.11)
- **Deployment**: Multi-container with separate webserver, scheduler, and triggerer
- **Executor**: LocalExecutor
- **Compose file**: `docker-compose.yml`
- **CLI syntax**: Uses `-d` flag for DAG ID (e.g., `airflow dags list-runs -d test_dag`)

### Airflow 3.x (3.1.5-python3.12)
- **Deployment**: Standalone mode (all components in one container)
- **Architecture**: Task SDK with API server communication
- **Compose file**: `docker-compose.airflow-3-standalone.yml`
- **CLI syntax**: Uses positional arguments (e.g., `airflow dags list-runs test_dag`)
- **Why standalone?**: Multi-container deployment has Task SDK API communication issues

## Test Scenarios

Each test run now verifies reservation behavior for all supported BigQuery operator types:

- **BigQueryInsertJobOperator**
  - `bq_insert_job_task`: standard reservation injection
  - `bq_execute_query_task`: query-style insert job with reservation
  - `bq_ondemand_task`: on-demand reservation (`'none'`)
  - `bq_no_reservation_task`: no reservation (task not in config)
  - `my_group.nested_task`: nested task group reservation
  - `bq_insert_job_legacy_task`: legacy SQL configuration with reservation
- **BigQueryExecuteQueryOperator**
  - `bq_execute_query_op_applied`: re-assigned to configured reservation
  - `bq_execute_query_op_skipped`: pre-existing reservation, policy skip verified
- **BigQueryCheckOperator / BigQueryValueCheckOperator**
  - `bq_check_task`, `bq_value_check_task`: safety checks that ensure no SQL-based injection is applied
- **BigQueryGetDataOperator**
  - `bq_get_data_task`: data fetch using public dataset with reservation-aware policy handling
- **BigQueryIntervalCheckOperator**
  - `bq_interval_check_applied`, `bq_interval_check_skipped`: interval checks run without unsafe SQL injection
- **BigQueryColumnCheckOperator**
  - `bq_column_check_applied`, `bq_column_check_skipped`: column checks on a public table with safe execution
- **BigQueryTableCheckOperator**
  - `bq_table_check_applied`, `bq_table_check_skipped`: table-level checks with safe execution

For operators where a BigQuery `job_id` can be retrieved, the DAG includes downstream assertion
tasks that verify the corresponding BigQuery job completed successfully (and, where possible,
used the expected reservation). The shell harness (`verify_all_tasks`) continues to provide
log-based verification and debugging support.

## Common Library

The `lib/common.sh` library provides reusable functions:

- `wait_for_airflow_health()`: Wait for Airflow to be healthy
- `wait_for_dag()`: Wait for DAG to be parsed
- `trigger_dag()`: Trigger a DAG run
- `wait_for_dag_completion()`: Wait for DAG run to complete
- `get_latest_run_id()`: Get the most recent run ID
- `verify_task_log()`: Verify task log contains expected content
- `verify_all_tasks()`: Run all standard test verifications

These functions handle version-specific differences (e.g., CLI syntax) internally.

## Makefile Targets

```bash
make e2e           # Test with Airflow 2.x (default)
make e2e-airflow2  # Test with Airflow 2.x
make e2e-airflow3  # Test with Airflow 3.x
make e2e-all       # Test with both versions
make e2e-keep      # Test with Airflow 2.x, keep containers running
```

## Debugging

### Access Airflow UI

When using `--keep` flag:
- **URL**: http://localhost:8080
- **Credentials**: admin/admin

### Check Logs

```bash
# View all logs
docker compose -f docker-compose.yml logs

# Follow logs
docker compose -f docker-compose.yml logs -f

# View specific container logs
docker compose -f docker-compose.yml logs airflow-scheduler
```

### Manual Cleanup

```bash
# Airflow 2.x
docker compose -f docker-compose.yml down -v

# Airflow 3.x
docker compose -f docker-compose.airflow-3-standalone.yml down -v
```

## Development

### Adding New Test Scenarios

1. Update `dags/test_reservation_dag.py` with new tasks
2. Update `reservations_config.json` if needed
3. Add verification logic to `lib/common.sh` in `verify_all_tasks()`

### Testing New Airflow Versions

1. Create a new docker-compose file (e.g., `docker-compose.airflow-4.yml`)
2. Add version configuration to `run_e2e_test.sh`
3. Handle any CLI syntax differences in `lib/common.sh`
