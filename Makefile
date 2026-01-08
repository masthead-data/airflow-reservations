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

.venv:
	python3 -m venv .venv

install: .venv
	.venv/bin/pip install -e .

install-dev: .venv
	.venv/bin/pip install -e ".[dev]"

test:
	.venv/bin/pytest tests/ -v

test-cov:
	.venv/bin/pytest tests/ --cov=airflow_reservations_policy --cov-report=term-missing

lint:
	.venv/bin/python -m black --check src/ tests/

format:
	.venv/bin/python -m black src/ tests/

e2e: e2e-airflow2 e2e-airflow3
	@echo ""
	@echo "✅ All E2E tests passed for both Airflow 2.x and 3.x!"

e2e-airflow2:
	@echo "Running E2E tests with Airflow 2.x..."
	./tests/e2e/run_e2e_test.sh --version 2

e2e-airflow3:
	@echo "Running E2E tests with Airflow 3.x..."
	./tests/e2e/run_e2e_test.sh --version 3

e2e-keep:
	./tests/e2e/run_e2e_test.sh --version 2 --keep

clean:
	rm -rf build/ dist/ *.egg-info src/*.egg-info .pytest_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
