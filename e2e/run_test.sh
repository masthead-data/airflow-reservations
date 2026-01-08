#!/bin/bash
#
# E2E Test Runner for Airflow Reservations Policy
#
# This script:
# 1. Starts Airflow in Docker with the plugin installed from local source
# 2. Waits for Airflow to be healthy
# 3. Triggers the test DAG
# 4. Waits for completion and checks logs for reservation injection
# 5. Cleans up
#
# Usage:
#   ./e2e/run_test.sh                                      # Test with Airflow 2.x (default)
#   COMPOSE_FILE=docker-compose.airflow-3.yml ./e2e/run_test.sh  # Test with Airflow 3.x
#
# Options:
#   --keep    Don't tear down containers after test (for debugging)
#   --logs    Show full container logs
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
E2E_DIR="$SCRIPT_DIR"

# Default to Airflow 2.x compose file if not specified
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

KEEP_CONTAINERS=false
SHOW_LOGS=false

# Parse arguments
for arg in "$@"; do
    case $arg in
        --keep)
            KEEP_CONTAINERS=true
            ;;
        --logs)
            SHOW_LOGS=true
            ;;
    esac
done

cd "$E2E_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    if [ "$KEEP_CONTAINERS" = false ]; then
        log_info "Cleaning up containers..."
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    else
        log_info "Keeping containers running (--keep flag)"
        log_info "Access Airflow UI at: http://localhost:8080 (admin/admin)"
        log_info "To clean up later: cd e2e && docker compose -f $COMPOSE_FILE down -v"
    fi
}

# Set up trap for cleanup on exit
if [ "$KEEP_CONTAINERS" = false ]; then
    trap cleanup EXIT
fi

log_info "=========================================="
log_info "Airflow Reservations Policy - E2E Test"
log_info "Compose File: $COMPOSE_FILE"
log_info "=========================================="

# Create logs directory
mkdir -p logs

# Clean up any existing containers
log_info "Cleaning up existing containers..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

# Set AIRFLOW_UID for Linux compatibility
export AIRFLOW_UID=$(id -u)

# Start services
log_info "Starting Airflow services..."
docker compose -f "$COMPOSE_FILE" up -d

# Wait for webserver to be healthy
log_info "Waiting for Airflow to be ready..."
MAX_WAIT=300
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    # Check health endpoints for both Airflow 2.x (/health) and 3.x (/api/v2/monitor/health)
    if curl -s http://localhost:8080/health 2>/dev/null | grep -q '"status": "healthy"' || \
       curl -s http://localhost:8080/api/v2/monitor/health 2>/dev/null | grep -q '"status":"healthy"'; then
        log_info "Airflow is healthy!"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    echo -n "."
done
echo ""

if [ $WAITED -ge $MAX_WAIT ]; then
    log_error "Airflow failed to become healthy within ${MAX_WAIT}s"
    if [ "$SHOW_LOGS" = true ]; then
        docker compose -f "$COMPOSE_FILE" logs
    fi
    exit 1
fi

# Wait for DAG to be parsed
log_info "Waiting for DAG to be parsed..."
# Force DAG reserialization to ensure DAGs are loaded (Airflow 3.x may need this)
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler airflow dags reserialize >/dev/null 2>&1 || true
sleep 5

MAX_DAG_WAIT=60
DAG_WAITED=0
while [ $DAG_WAITED -lt $MAX_DAG_WAIT ]; do
    if docker compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags list 2>/dev/null | grep -q "test_reservation_dag"; then
        log_info "DAG found!"
        break
    fi
    sleep 5
    DAG_WAITED=$((DAG_WAITED + 5))
    echo -n "."
done
echo ""

if [ $DAG_WAITED -ge $MAX_DAG_WAIT ]; then
    log_error "DAG not found within ${MAX_DAG_WAIT}s"
    exit 1
fi

# Trigger the test DAG
log_info "Triggering test DAG..."
docker compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags trigger test_reservation_dag

# Wait for DAG run to complete
log_info "Waiting for DAG run to complete..."
MAX_WAIT=90
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    STATUS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags list-runs -d test_reservation_dag -o json 2>/dev/null | python3 -c "import sys, json; runs=json.load(sys.stdin); print(runs[0]['state'] if runs else 'none')" 2>/dev/null || echo "pending")

    if [ "$STATUS" = "success" ]; then
        log_info "DAG run completed successfully!"
        break
    elif [ "$STATUS" = "failed" ]; then
        log_error "DAG run failed!"
        break
    fi

    sleep 5
    WAITED=$((WAITED + 5))
    echo -n "."
done
echo ""

if [ $WAITED -ge $MAX_WAIT ]; then
    log_warn "DAG run did not complete within ${MAX_WAIT}s"
fi

# Get the Run ID provided by Airflow
RUN_ID=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags list-runs -d test_reservation_dag -o json 2>/dev/null | python3 -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])" 2>/dev/null)
log_info "Using Run ID: $RUN_ID"

# Check task logs for reservation injection
log_info "=========================================="
log_info "Checking task logs for reservation injection..."
log_info "=========================================="

# Read log files directly from the filesystem
echo ""
log_info "Task 1: bq_insert_job_task (should have reservation path)"
echo "---"
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_insert_job_task/attempt=1.log" 2>/dev/null | grep -E "(SET @@reservation_id|SUCCESS|FAILURE|SQL Query)" || true

echo ""
log_info "Task 2: bq_execute_query_task (should have reservation path)"
echo "---"
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_execute_query_task/attempt=1.log" 2>/dev/null | grep -E "(SET @@reservation_id|SUCCESS|FAILURE|SQL Query)" || true

echo ""
log_info "Task 3: bq_ondemand_task (should have reservation = 'none' for on-demand)"
echo "---"
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_ondemand_task/attempt=1.log" 2>/dev/null | grep -E "(SET @@reservation_id|SUCCESS|FAILURE|SQL Query)" || true

echo ""
log_info "Task 4: bq_no_reservation_task (should NOT have any reservation)"
echo "---"
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_no_reservation_task/attempt=1.log" 2>/dev/null | grep -E "(SET @@reservation_id|SUCCESS|FAILURE|SQL Query)" || true

echo ""
log_info "Task 5: my_group.nested_task (should have reservation path)"
echo "---"
docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=my_group.nested_task/attempt=1.log" 2>/dev/null | grep -E "(SET @@reservation_id|SUCCESS|FAILURE|SQL Query)" || true

echo ""
log_info "=========================================="

# Verify results - read log files directly
TASK1_LOGS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_insert_job_task/attempt=1.log" 2>/dev/null || true)
TASK2_LOGS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_execute_query_task/attempt=1.log" 2>/dev/null || true)
TASK3_LOGS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_ondemand_task/attempt=1.log" 2>/dev/null || true)
TASK4_LOGS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=bq_no_reservation_task/attempt=1.log" 2>/dev/null || true)
TASK5_LOGS=$(docker compose -f "$COMPOSE_FILE" exec -T airflow-scheduler cat "/opt/airflow/logs/dag_id=test_reservation_dag/run_id=$RUN_ID/task_id=my_group.nested_task/attempt=1.log" 2>/dev/null || true)

PASSED=0
FAILED=0

# Task 1: Should have the reservation path injected
if echo "$TASK1_LOGS" | grep -q "e2e-test-reservation"; then
    log_info "✅ Task 1: Reservation path correctly injected"
    PASSED=$((PASSED + 1))
else
    log_error "❌ Task 1: Reservation path NOT found"
    FAILED=$((FAILED + 1))
fi

# Task 2: Should have the reservation path injected
if echo "$TASK2_LOGS" | grep -q "e2e-test-reservation"; then
    log_info "✅ Task 2: Reservation path correctly injected"
    PASSED=$((PASSED + 1))
else
    log_error "❌ Task 2: Reservation path NOT found"
    FAILED=$((FAILED + 1))
fi

# Task 3: Should have 'none' for on-demand capacity
if echo "$TASK3_LOGS" | grep -q "= 'none'"; then
    log_info "✅ Task 3: On-demand reservation ('none') correctly injected"
    PASSED=$((PASSED + 1))
else
    log_error "❌ Task 3: On-demand reservation ('none') NOT found"
    FAILED=$((FAILED + 1))
fi

# Task 4: Should NOT have any reservation (not in config)
if echo "$TASK4_LOGS" | grep -q "SET @@reservation_id"; then
    log_error "❌ Task 4: Unexpected reservation found (should not have one)"
    FAILED=$((FAILED + 1))
else
    log_info "✅ Task 4: Correctly has no reservation (not in config)"
    PASSED=$((PASSED + 1))
fi

# Task 5: Should have the nested reservation path injected
if echo "$TASK5_LOGS" | grep -q "e2e-test-reservation"; then
    log_info "✅ Task 5: Nested task reservation correctly injected"
    PASSED=$((PASSED + 1))
else
    log_info "Checking alternative log path for Task 5..."
    # Fallback debug info
    docker compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow tasks list test_reservation_dag 2>/dev/null | grep nested || true

    log_error "❌ Task 5: Nested task reservation NOT found"
    log_error "Logs content: $(echo "$TASK5_LOGS" | head -n 5)..."
    FAILED=$((FAILED + 1))
fi

echo ""
log_info "=========================================="
log_info "Test Results: ${PASSED} passed, ${FAILED} failed"
log_info "=========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi

log_info "All E2E tests passed! 🎉"
