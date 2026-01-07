.PHONY: install install-dev test test-cov lint format e2e clean help setup

# Default target
help:
	@echo "Available targets:"
	@echo "  setup        Install package with dev dependencies (for CI)"
	@echo "  install      Install package"
	@echo "  install-dev  Install package with dev dependencies"
	@echo "  test         Run unit tests"
	@echo "  test-cov     Run tests with coverage"
	@echo "  lint         Run linting"
	@echo "  format       Format code"
	@echo "  e2e          Run E2E tests (requires Docker)"
	@echo "  clean        Clean build artifacts"

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

e2e-keep:
	./e2e/run_test.sh --keep

clean:
	rm -rf build/ dist/ *.egg-info src/*.egg-info .pytest_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
