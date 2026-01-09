#!/bin/bash
#
# Common utilities for E2E tests
#

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

# Wait for Airflow to be healthy
# Args: $1 = compose_file, $2 = max_wait_seconds (optional, default 120)
wait_for_airflow_health() {
    local compose_file="$1"
    local max_wait="${2:-120}"
    local waited=0

    log_info "Waiting for Airflow to be ready..."

    while [ $waited -lt $max_wait ]; do
        # Check health endpoints for both Airflow 2.x (/health) and 3.x (/api/v2/monitor/health)
        if curl -s http://localhost:8080/health 2>/dev/null | grep -q '"status": "healthy"' || \
           curl -s http://localhost:8080/api/v2/monitor/health 2>/dev/null | grep -q '"status":"healthy"'; then
            log_info "Airflow is healthy!"
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
        echo -n "."
    done
    echo ""

    log_error "Airflow failed to become healthy within ${max_wait}s"
    return 1
}

# Wait for DAG to be parsed
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id, $4 = max_wait_seconds (optional, default 60)
wait_for_dag() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"
    local max_wait="${4:-60}"
    local waited=0

    log_info "Waiting for DAG to be parsed..."

    # Try to force DAG reserialization (works in Airflow 2.x, may help in 3.x)
    docker compose -f "$compose_file" exec -T "$container_name" airflow dags reserialize >/dev/null 2>&1 || true
    sleep 5

    while [ $waited -lt $max_wait ]; do
        if docker compose -f "$compose_file" exec -T "$container_name" airflow dags list 2>/dev/null | grep -q "$dag_id"; then
            log_info "DAG found!"
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
        echo -n "."
    done
    echo ""

    log_error "DAG not found within ${max_wait}s"
    return 1
}

# Trigger DAG
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id
trigger_dag() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"

    log_info "Triggering test DAG..."
    docker compose -f "$compose_file" exec -T "$container_name" airflow dags trigger "$dag_id" >/dev/null 2>&1
}

# Wait for DAG run to complete
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id, $4 = max_wait_seconds (optional, default 90), $5 = airflow_version (2 or 3)
# Returns: 0 if success, 1 if failed, 2 if timeout
wait_for_dag_completion() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"
    local max_wait="${4:-90}"
    local airflow_version="${5:-3}"
    local waited=0
    local status=""

    log_info "Waiting for DAG run to complete..."

    while [ $waited -lt $max_wait ]; do
        # Airflow 2.x uses -d flag, Airflow 3.x uses positional argument
        local output=""
        if [ "$airflow_version" = "2" ]; then
            output=$(docker compose -f "$compose_file" exec -T "$container_name" airflow dags list-runs -d "$dag_id" -o json 2>/dev/null || echo "")
        else
            output=$(docker compose -f "$compose_file" exec -T "$container_name" airflow dags list-runs "$dag_id" -o json 2>/dev/null || echo "")
        fi

        if [ -n "$output" ]; then
            status=$(echo "$output" | grep -o '"state": "[^"]*"' | head -1 | cut -d'"' -f4)

            if [ "$status" = "success" ]; then
                log_info "DAG run completed successfully!"
                return 0
            elif [ "$status" = "failed" ]; then
                log_error "DAG run failed!"

                # Show task states for debugging
                log_info "Fetching task states for debugging..."
                local run_id=$(get_latest_run_id "$compose_file" "$container_name" "$dag_id" "$airflow_version")
                if [ "$airflow_version" = "2" ]; then
                    docker compose -f "$compose_file" exec -T "$container_name" \
                        airflow tasks states-for-dag-run "$dag_id" "$run_id" 2>/dev/null || true
                else
                    docker compose -f "$compose_file" exec -T "$container_name" \
                        airflow tasks states-for-dag-run "$dag_id" "$run_id" 2>/dev/null || true
                fi

                # Show logs for failed tasks
                log_info "Fetching logs for failed tasks..."
                local log_path="/opt/airflow/logs/dag_id=${dag_id}/run_id=${run_id}"
                docker compose -f "$compose_file" exec -T "$container_name" \
                    find "$log_path" -name "*.log" -type f 2>/dev/null | while read log_file; do
                        log_info "=== Log: $log_file ==="
                        docker compose -f "$compose_file" exec -T "$container_name" cat "$log_file" 2>/dev/null || true
                    done

                return 1
            fi
        fi

        sleep 5
        waited=$((waited + 5))
        echo -n "."
    done
    echo ""

    log_warn "DAG run did not complete within ${max_wait}s"
    return 2
}

# Get the most recent run ID for a DAG
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id, $4 = airflow_version (2 or 3)
# Outputs: run_id to stdout
get_latest_run_id() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"
    local airflow_version="${4:-3}"

    if [ "$airflow_version" = "2" ]; then
        docker compose -f "$compose_file" exec -T "$container_name" \
            airflow dags list-runs -d "$dag_id" -o json 2>/dev/null | \
            grep -o '"run_id": "[^"]*"' | head -1 | cut -d'"' -f4
    else
        docker compose -f "$compose_file" exec -T "$container_name" \
            airflow dags list-runs "$dag_id" -o json 2>/dev/null | \
            grep -o '"run_id": "[^"]*"' | head -1 | cut -d'"' -f4
    fi
}

# Verify task log contains expected reservation
# Args: $1 = compose_file, $2 = log_container, $3 = dag_id, $4 = run_id, $5 = task_id, $6 = search_pattern, $7 = should_find (true/false)
# Returns: 0 if verification passes, 1 if fails
verify_task_log() {
    local compose_file="$1"
    local log_container="$2"
    local dag_id="$3"
    local run_id="$4"
    local task_id="$5"
    local search_pattern="$6"
    local should_find="${7:-true}"

    local log_path="/opt/airflow/logs/dag_id=${dag_id}/run_id=${run_id}/task_id=${task_id}/attempt=1.log"
    local logs=$(docker compose -f "$compose_file" exec -T "$log_container" cat "$log_path" 2>/dev/null || echo "")

    if [ "$should_find" = "true" ]; then
        if echo "$logs" | grep -q "$search_pattern"; then
            return 0
        else
            return 1
        fi
    else
        if echo "$logs" | grep -q "$search_pattern"; then
            return 1
        else
            return 0
        fi
    fi
}

# Run verification for all standard test tasks
# Args: $1 = compose_file, $2 = log_container, $3 = dag_id, $4 = run_id
# Returns: number of failed tests
verify_all_tasks() {
    local compose_file="$1"
    local log_container="$2"
    local dag_id="$3"
    local run_id="$4"
    local passed=0
    local failed=0

    log_info "=========================================="
    log_info "Verifying reservation injection..."
    log_info "=========================================="

    # Task 1: Should have reservation path
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_insert_job_task" "e2e-test-reservation" "true"; then
        log_info "✅ Task 1: Reservation injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 1: Reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 2: Should have reservation path
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_task" "e2e-test-reservation" "true"; then
        log_info "✅ Task 2: Reservation injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 2: Reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 3: Should have 'none' for on-demand
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_ondemand_task" "= 'none'" "true"; then
        log_info "✅ Task 3: On-demand reservation ('none') injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 3: On-demand reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 4: Should NOT have reservation (not in config)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_no_reservation_task" "SET @@reservation_id" "false"; then
        log_info "✅ Task 4: Correctly has no reservation"
        passed=$((passed + 1))
    else
        log_error "❌ Task 4: Unexpected reservation found"
        failed=$((failed + 1))
    fi

    # Task 5: Nested task should have reservation
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "my_group.nested_task" "e2e-test-reservation" "true"; then
        log_info "✅ Task 5: Nested task reservation injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 5: Nested task reservation NOT found"
        failed=$((failed + 1))
    fi

    log_info "=========================================="
    log_info "Test Results: ${passed} passed, ${failed} failed"
    log_info "=========================================="

    return $failed
}
