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

```text
tests/e2e/
├── lib/
│   └── common.sh              # Shared test utilities and functions
├── run_e2e_test.sh            # Unified test runner (supports both versions)
├── docker-compose.yml         # Airflow 2.x multi-container setup
├── docker-compose.airflow-3-standalone.yml  # Airflow 3.x standalone setup
├── dags/                      # Test DAGs and configuration
│   ├── test_reservation_dag.py         # Test DAG with multiple operator types and assertion tasks
│   └── reservations_config.json        # Reservation mappings for test tasks
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
- **BigQueryExecuteQueryOperator**: Not available (removed in provider v11.0.0+)
- **Why standalone?**: Multi-container deployment has Task SDK API communication issues

## Test Scenarios

The test DAG verifies reservation behavior across all supported BigQuery operator types:

- BigQueryInsertJobOperator
    - **Standard SQL with reservation**: Verifies reservation injection via `configuration.reservation`
    - **Nested in TaskGroup**: Confirms reservation injection works for tasks in task groups
    - **On-demand (`'none'`)**: Validates explicit on-demand (no reservation) configuration
    - **No reservation (`null`)**: Confirms no injection when task is not configured

- BigQueryExecuteQueryOperator (Provider 2.0.0-10.26.0 only)
    - **Standard SQL with reservation**: Verifies injection via `api_resource_configs.reservation`
    - **Pre-configured reservation**: Ensures manually set reservations are preserved (not overwritten)
    - **Conditional loading**: Automatically skipped in Airflow 3.x or unsupported provider versions

- PythonOperator with Custom BigQuery Client

Testing combines two-layer verification methods:
1. **Log-based verification** (`verify_all_tasks` in shell): Checks Airflow logs for injection messages
2. **BigQuery API verification** (assertion tasks in DAG): Queries BigQuery API to confirm actual reservation usage

## Common Library

The `lib/common.sh` library provides reusable functions with built-in version compatibility:

- `wait_for_airflow_health()`: Wait for Airflow to be healthy
- `wait_for_dag()`: Wait for DAG to be parsed
- `trigger_dag()`: Trigger a DAG run
- `wait_for_dag_completion()`: Wait for DAG run to complete
- `get_latest_run_id()`: Get the most recent run ID
- `verify_task_log()`: Verify task log contains expected content
- `verify_all_tasks()`: Run all standard test verifications
  - Handles conditional operator availability (e.g., BigQueryExecuteQueryOperator)
  - Skips log verification for BigQueryExecuteQueryOperator that doesn't log details
  - Checks for orphaned configuration entries

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

1. Add new task to `dags/test_reservation_dag.py`
2. Add corresponding assertion task (if task returns job_id)
3. Update `dags/reservations_config.json` with task reservation mapping
4. Verify log-based checks work in `lib/common.sh` `verify_all_tasks()` function
5. Run tests: `make e2e`

### Testing New Airflow Versions

1. Create a new docker-compose file (e.g., `docker-compose.airflow-4.yml`)
2. Add version configuration to `run_e2e_test.sh`
3. Handle any CLI syntax differences in `lib/common.sh`
