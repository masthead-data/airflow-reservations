# E2E Test Environment

This directory contains an end-to-end test environment for the airflow-reservations-policy plugin.

## Quick Start

```bash
# Test with Airflow 2.x (default)
./e2e/run_test.sh

# Test with Airflow 3.x
COMPOSE_FILE=docker-compose.airflow-3.yml ./e2e/run_test.sh

# Or use Makefile targets
make e2e-airflow2  # Test with Airflow 2.x
make e2e-airflow3  # Test with Airflow 3.x
make e2e-all       # Test with all supported versions
```

This single command will:
1. Start Airflow in Docker with the plugin installed from local source
2. Wait for Airflow to be healthy
3. Trigger the test DAG
4. Verify reservation injection in task logs
5. Clean up containers

## Supported Versions

The e2e tests run against multiple Airflow versions to ensure compatibility:

| Compose File | Airflow Version | Python Version | Status |
|--------------|-----------------|----------------|--------|
| `docker-compose.yml` (default) | 2.10.4 | 3.11 | ✅ Tested |
| `docker-compose.airflow-3.yml` | 3.1.5 | 3.12 | ✅ Tested |
1. Start Airflow in Docker with the plugin installed from local source
2. Wait for Airflow to be healthy
3. Trigger the test DAG
4. Verify reservation injection in task logs
5. Clean up containers

## Options

```bash
# Keep containers running after test (for debugging)
./e2e/run_test.sh --keep

# Show full container logs
./e2e/run_test.sh --logs
```

## Access Airflow UI

When using `--keep`, you can access:
- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

## Manual Cleanup

```bash
cd e2e && docker compose down -v
```

## What the Test Verifies

The test DAG has 3 tasks:

| Task                     | Expected Result                             |
| ------------------------ | ------------------------------------------- |
| `bq_insert_job_task`     | Should have reservation injected            |
| `bq_execute_query_task`  | Should have reservation injected            |
| `bq_no_reservation_task` | Should NOT have reservation (not in config) |

## Files

- `docker-compose.yml` - Airflow services configuration
- `dags/test_reservation_dag.py` - Test DAG with mock BigQuery operators
- `dags/reservations_config.json` - Reservation mappings for test tasks
- `run_test.sh` - Test runner script
