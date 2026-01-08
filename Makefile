.PHONY: install install-dev test test-cov lint format e2e e2e-airflow2 e2e-airflow3 e2e-all clean help setup

# Default target
help:
	@echo "Available targets:"
	@echo "  setup          Install package with dev dependencies (for CI)"
	@echo "  install        Install package"
	@echo "  install-dev    Install package with dev dependencies"
	@echo "  test           Run unit tests"
	@echo "  test-cov       Run tests with coverage"
	@echo "  lint           Run linting"
	@echo "  format         Format code"
	@echo "  e2e            Run E2E tests with Airflow 2.x (default, requires Docker)"
	@echo "  e2e-airflow2   Run E2E tests with Airflow 2.x (requires Docker)"
	@echo "  e2e-airflow3   Run E2E tests with Airflow 3.x (requires Docker)"
	@echo "  e2e-all        Run E2E tests with all Airflow versions (requires Docker)"
	@echo "  clean          Clean build artifacts"

setup: install-dev

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=airflow_reservations_policy --cov-report=term-missing

lint:
	black --check src/ tests/

format:
	black src/ tests/

e2e:
	./e2e/run_test.sh

e2e-airflow2:
	./e2e/run_test.sh

e2e-airflow3:
	COMPOSE_FILE=docker-compose.airflow-3.yml ./e2e/run_test.sh

e2e-all:
	@echo "Running E2E tests with Airflow 2.x..."
	./e2e/run_test.sh
	@echo ""
	@echo "Running E2E tests with Airflow 3.x..."
	COMPOSE_FILE=docker-compose.airflow-3.yml ./e2e/run_test.sh
	@echo ""
	@echo "✅ All E2E tests passed for both Airflow 2.x and 3.x!"

e2e-keep:
	./e2e/run_test.sh --keep

clean:
	rm -rf build/ dist/ *.egg-info src/*.egg-info .pytest_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
