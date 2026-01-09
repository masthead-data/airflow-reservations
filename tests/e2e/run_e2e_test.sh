#!/bin/bash
#
# Unified E2E Test Runner for Airflow Reservations Policy
#
# This script supports testing with different Airflow versions using a common framework.
#
# Usage:
#   ./e2e/run_e2e_test.sh --version 2                      # Test with Airflow 2.x
#   ./e2e/run_e2e_test.sh --version 3                      # Test with Airflow 3.x
#   ./e2e/run_e2e_test.sh --version 2 --keep               # Keep containers running
#   ./e2e/run_e2e_test.sh --version 3 --logs               # Show full logs on failure
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source common utilities
source "$SCRIPT_DIR/lib/common.sh"

# Default values
AIRFLOW_VERSION="2"
KEEP_CONTAINERS=false
SHOW_LOGS=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version|-v)
            AIRFLOW_VERSION="$2"
            shift 2
            ;;
        --keep|-k)
            KEEP_CONTAINERS=true
            shift
            ;;
        --logs|-l)
            SHOW_LOGS=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --version, -v VERSION   Airflow version to test (2 or 3, default: 2)"
            echo "  --keep, -k              Keep containers running after test"
            echo "  --logs, -l              Show full container logs on failure"
            echo "  --help, -h              Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Configure based on version
case $AIRFLOW_VERSION in
    2)
        COMPOSE_FILE="docker-compose.yml"
        WEBSERVER_CONTAINER="airflow-webserver"
        SCHEDULER_CONTAINER="airflow-scheduler"
        LOG_CONTAINER="airflow-scheduler"
        DESCRIPTION="Airflow 2.x (Multi-container)"
        ;;
    3)
        COMPOSE_FILE="docker-compose.airflow-3-standalone.yml"
        WEBSERVER_CONTAINER="airflow-standalone"
        SCHEDULER_CONTAINER="airflow-standalone"
        LOG_CONTAINER="airflow-standalone"
        DESCRIPTION="Airflow 3.x (Standalone)"
        ;;
    *)
        log_error "Unsupported Airflow version: $AIRFLOW_VERSION"
        log_error "Supported versions: 2, 3"
        exit 1
        ;;
esac

DAG_ID="test_reservation_dag"

log_info "=========================================="
log_info "Airflow Reservations Policy - E2E Test"
log_info "Version: $DESCRIPTION"
log_info "=========================================="

# Cleanup function
cleanup() {
    if [ "$KEEP_CONTAINERS" = false ]; then
        log_info "Cleaning up containers..."
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    else
        log_info "Keeping containers running (--keep flag)"
        log_info "Access Airflow UI at: http://localhost:8080"
        log_info "To clean up later: docker compose -f $COMPOSE_FILE down -v"
    fi
}

# Set up trap for cleanup on exit
if [ "$KEEP_CONTAINERS" = false ]; then
    trap cleanup EXIT
fi

# Clean up any existing containers
log_info "Cleaning up existing containers..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

# Start services
log_info "Starting Airflow services..."
export AIRFLOW_UID=$(id -u)
docker compose -f "$COMPOSE_FILE" up -d

# Wait for Airflow to be healthy
if ! wait_for_airflow_health "$COMPOSE_FILE" 120; then
    if [ "$SHOW_LOGS" = true ]; then
        docker compose -f "$COMPOSE_FILE" logs
    fi
    exit 1
fi

# Create google_cloud_default connection
log_info "Creating google_cloud_default connection..."
docker compose -f "$COMPOSE_FILE" exec "$SCHEDULER_CONTAINER" airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' 2>/dev/null || true

# Wait for DAG to be parsed
if [ "$AIRFLOW_VERSION" = "3" ]; then
    # Airflow 3.x needs more time for initial parsing
    log_info "Waiting for initial DAG parsing..."
    sleep 20
fi

if ! wait_for_dag "$COMPOSE_FILE" "$SCHEDULER_CONTAINER" "$DAG_ID" 60; then
    if [ "$SHOW_LOGS" = true ]; then
        docker compose -f "$COMPOSE_FILE" logs
    fi
    exit 1
fi

# Trigger the DAG
trigger_dag "$COMPOSE_FILE" "$WEBSERVER_CONTAINER" "$DAG_ID"

# Wait for completion
wait_for_dag_completion "$COMPOSE_FILE" "$WEBSERVER_CONTAINER" "$DAG_ID" 90 "$AIRFLOW_VERSION"
completion_status=$?

if [ $completion_status -ne 0 ]; then
    if [ "$SHOW_LOGS" = true ]; then
        docker compose -f "$COMPOSE_FILE" logs
    fi
    if [ $completion_status -eq 1 ]; then
        log_error "DAG run failed"
        exit 1
    else
        log_error "DAG run timed out"
        exit 1
    fi
fi

# Get run ID
RUN_ID=$(get_latest_run_id "$COMPOSE_FILE" "$WEBSERVER_CONTAINER" "$DAG_ID" "$AIRFLOW_VERSION")
log_info "Checking logs for run: $RUN_ID"

# Verify all tasks
verify_all_tasks "$COMPOSE_FILE" "$LOG_CONTAINER" "$DAG_ID" "$RUN_ID"
verification_result=$?

if [ $verification_result -ne 0 ]; then
    log_error "$verification_result test(s) failed"
    exit 1
fi

log_info "All E2E tests passed! 🎉"
