# Contributing

## Development tasks

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=airflow_reservations

# Run E2E tests (requires Docker)
make e2e

# Run E2E tests with Airflow 2.x
make e2e-airflow2

# Run E2E tests with Airflow 3.x
make e2e-airflow3

# Run E2E tests with all supported Airflow versions
make e2e-all
```
