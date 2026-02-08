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
# Args: $1 = compose_file, $2 = max_wait_seconds (optional, default 300), $3 = airflow_version (optional)
wait_for_airflow_health() {
    local compose_file="$1"
    local max_wait="${2:-300}"
    local airflow_version="${3:-3}"
    local waited=0

    log_info "Waiting for Airflow to be ready..."

    while [ $waited -lt $max_wait ]; do
        if [ "$waited" -gt 0 ] && [ $((waited % 30)) -eq 0 ]; then
            log_info "Still waiting for Airflow... (${waited}s/${max_wait}s)"
        fi
        if [ "$airflow_version" = "2" ]; then
            # For Airflow 2.x, check scheduler container health via docker compose
            if docker compose -f "$compose_file" ps | grep -q "scheduler.*healthy"; then
                log_info "Airflow is healthy!"
                return 0
            fi
        else
            # For Airflow 3.x standalone, check health endpoint
            if curl -s http://localhost:8080/api/v2/monitor/health 2>/dev/null | grep -q '"status":"healthy"'; then
                log_info "Airflow is healthy!"
                return 0
            fi
        fi
        sleep 2
        waited=$((waited + 2))
        echo -n "."
    done
    echo ""

    log_error "Airflow failed to become healthy within ${max_wait}s"
    log_info "Recent container logs:"
    docker compose -f "$compose_file" logs --tail=50
    return 1
}

# Wait for DAG to be parsed
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id, $4 = max_wait_seconds (optional, default 120)
wait_for_dag() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"
    local max_wait="${4:-120}"
    local waited=0

    log_info "Waiting for DAG to be parsed..."

    # Try to force DAG reserialization (works in Airflow 2.x, may help in 3.x)
    docker compose -f "$compose_file" exec -T "$container_name" airflow dags reserialize >/dev/null 2>&1 || true

    while [ $waited -lt $max_wait ]; do
        if docker compose -f "$compose_file" exec -T "$container_name" airflow dags list 2>/dev/null | grep -q "$dag_id"; then
            log_info "DAG found!"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
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
# Args: $1 = compose_file, $2 = container_name, $3 = dag_id, $4 = max_wait_seconds (optional, default 180), $5 = airflow_version (2 or 3)
# Returns: 0 if success, 1 if failed, 2 if timeout
wait_for_dag_completion() {
    local compose_file="$1"
    local container_name="$2"
    local dag_id="$3"
    local max_wait="${4:-180}"
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
                log_info "Fetching logs for all failed tasks..."
                local log_path="/opt/airflow/logs/dag_id=${dag_id}/run_id=${run_id}"

                # First, check if log directory exists
                if ! docker compose -f "$compose_file" exec -T "$container_name" test -d "$log_path" 2>/dev/null; then
                    log_error "Log directory not found: $log_path"
                    log_info "Determining why initialization failed..."
                    docker compose -f "$compose_file" logs --tail=100 2>&1 | grep -B 5 -A 10 -E "(ERROR|Exception|Traceback|Failed)" | tail -80
                else
                    # Find and display logs for all tasks that didn't succeed
                    # We first get the list of non-success tasks
                    local failed_tasks=$(docker compose -f "$compose_file" exec -T "$container_name" \
                        airflow tasks states-for-dag-run "$dag_id" "$run_id" 2>/dev/null | grep -E "(failed|upstream_failed|skipped|retry)" | awk '{print $5}')
                    
                    if [ -z "$failed_tasks" ]; then
                         # Fallback if the command above failed or returned nothing
                         log_info "Could not determine failed tasks list. Showing all available task logs."
                         for log_file in $(docker compose -f "$compose_file" exec -T "$container_name" find "$log_path" -name "*.log" -type f); do
                            log_info "=== Log for $log_file ==="
                            docker compose -f "$compose_file" exec -T "$container_name" cat "$log_file" | tail -50
                         done
                    else
                        for task in $failed_tasks; do
                            log_info "=== Logs for failed task: $task ==="
                            # Find all attempts for this task
                            local task_logs=$(docker compose -f "$compose_file" exec -T "$container_name" \
                                find "$log_path" -name "task_id=${task}*" -type d 2>/dev/null)
                            for tlog in $task_logs; do
                                docker compose -f "$compose_file" exec -T "$container_name" find "$tlog" -name "*.log" -type f -exec echo "--- Log file: {} ---" \; -exec cat {} \; | tail -100
                            done
                        done
                    fi
                fi

                return 1
            fi
        fi

        sleep 2
        waited=$((waited + 2))
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

    # Task 1: BigQueryInsertJobOperator (should use reservationId in configuration)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_insert_job_task" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "true"; then
        log_info "✅ Task 1: reservationId injected into BigQueryInsertJobOperator"
        passed=$((passed + 1))
    else
        log_error "❌ Task 1: reservationId NOT found in BigQueryInsertJobOperator configuration"
        failed=$((failed + 1))
    fi

    # Task 2: BigQueryExecuteQueryOperator (fallback to SQL if no configuration found in old versions)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_task" "SET @@reservation='projects/masthead-dev/locations/US/reservations/capacity-1'" "true" || \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_task" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "true"; then
        log_info "✅ Task 2: Reservation injected into BigQueryExecuteQueryOperator"
        passed=$((passed + 1))
    else
        log_error "❌ Task 2: Reservation NOT found in BigQueryExecuteQueryOperator"
        failed=$((failed + 1))
    fi

    # Task 3: Should have 'none' (in configuration or SQL)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_ondemand_task" "'reservationId': 'none'" "true" || \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_ondemand_task" "SET @@reservation='none'" "true"; then
        log_info "✅ Task 3: On-demand reservation ('none') injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 3: On-demand reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 4: Should NOT have reservation (neither config nor SQL)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_no_reservation_task" "'reservationId':" "false" && \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_no_reservation_task" "SET @@reservation" "false"; then
        log_info "✅ Task 4: Correctly has no reservation"
        passed=$((passed + 1))
    else
        log_error "❌ Task 4: Unexpected reservation found"
        failed=$((failed + 1))
    fi

    # Task 5: Nested task should have reservation
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "my_group.nested_task" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "true" || \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "my_group.nested_task" "SET @@reservation='projects/masthead-dev/locations/US/reservations/capacity-1'" "true"; then
        log_info "✅ Task 5: Nested task reservation injected"
        passed=$((passed + 1))
    else
        log_error "❌ Task 5: Nested task reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 6: BigQueryCheckOperator (Verifying SAFETY - should NOT have SQL injection)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_check_task" "SET @@reservation" "false"; then
        log_info "✅ Task 6: BigQueryCheckOperator is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 6: Unexpected SQL injection found in BigQueryCheckOperator"
        failed=$((failed + 1))
    fi

    # Task 7: BigQueryValueCheckOperator (Verifying SAFETY)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_value_check_task" "SET @@reservation" "false"; then
        log_info "✅ Task 7: BigQueryValueCheckOperator is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 7: Unexpected SQL injection found in BigQueryValueCheckOperator"
        failed=$((failed + 1))
    fi

    # Task 8: BigQueryGetDataOperator (Verifying SAFETY and SUCCESS)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_get_data_task" "SET @@reservation" "false"; then
        log_info "✅ Task 8: BigQueryGetDataOperator is safe and succeeded"
        passed=$((passed + 1))
    else
        log_error "❌ Task 8: Unexpected SQL injection found in BigQueryGetDataOperator"
        failed=$((failed + 1))
    fi

    # Task 9: Legacy SQL task (should use reservationId)
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_insert_job_legacy_task" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "true"; then
        log_info "✅ Task 9: Legacy SQL task has reservationId"
        passed=$((passed + 1))
    else
        log_error "❌ Task 9: Legacy SQL task reservationId NOT found"
        failed=$((failed + 1))
    fi

    # Task 10: Python custom BQ task should have reservation applied
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "python_custom_bq_task" "✅ SUCCESS: Reservation was applied in custom Python code!" "true"; then
        log_info "✅ Task 10: Python custom BigQuery task has reservation"
        passed=$((passed + 1))
    else
        log_error "❌ Task 10: Python custom BigQuery task reservation NOT found"
        failed=$((failed + 1))
    fi

    # Task 11: BigQueryExecuteQueryOperator (applied) should show reservation injection
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_op_applied" "SET @@reservation='projects/masthead-dev/locations/US/reservations/capacity-1'" "true" || \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_op_applied" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "true"; then
        log_info "✅ Task 11: Reservation injected into BigQueryExecuteQueryOperator (applied)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 11: Reservation NOT found for BigQueryExecuteQueryOperator (applied)"
        failed=$((failed + 1))
    fi

    # Task 12: BigQueryExecuteQueryOperator (skipped) should not receive our reservation
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_op_skipped" "SET @@reservation='projects/masthead-dev/locations/US/reservations/capacity-1'" "false" && \
       verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_execute_query_op_skipped" "'reservationId': 'projects/masthead-dev/locations/US/reservations/capacity-1'" "false"; then
        log_info "✅ Task 12: BigQueryExecuteQueryOperator (skipped) correctly not re-assigned"
        passed=$((passed + 1))
    else
        log_error "❌ Task 12: Unexpected reservation re-assignment for BigQueryExecuteQueryOperator (skipped)"
        failed=$((failed + 1))
    fi

    # Task 13: BigQueryIntervalCheckOperator (applied) – ensure no SQL injection artifacts
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_interval_check_applied" "SET @@reservation" "false"; then
        log_info "✅ Task 13: BigQueryIntervalCheckOperator (applied) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 13: Unexpected SQL injection found in BigQueryIntervalCheckOperator (applied)"
        failed=$((failed + 1))
    fi

    # Task 14: BigQueryIntervalCheckOperator (skipped) – also should have no SQL injection
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_interval_check_skipped" "SET @@reservation" "false"; then
        log_info "✅ Task 14: BigQueryIntervalCheckOperator (skipped) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 14: Unexpected SQL injection found in BigQueryIntervalCheckOperator (skipped)"
        failed=$((failed + 1))
    fi

    # Task 15: BigQueryColumnCheckOperator (applied) – ensure job ran without SQL injection
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_column_check_applied" "SET @@reservation" "false"; then
        log_info "✅ Task 15: BigQueryColumnCheckOperator (applied) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 15: Unexpected SQL injection found in BigQueryColumnCheckOperator (applied)"
        failed=$((failed + 1))
    fi

    # Task 16: BigQueryColumnCheckOperator (skipped) – same safety expectations
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_column_check_skipped" "SET @@reservation" "false"; then
        log_info "✅ Task 16: BigQueryColumnCheckOperator (skipped) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 16: Unexpected SQL injection found in BigQueryColumnCheckOperator (skipped)"
        failed=$((failed + 1))
    fi

    # Task 17: BigQueryTableCheckOperator (applied) – table checks run without SQL injection
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_table_check_applied" "SET @@reservation" "false"; then
        log_info "✅ Task 17: BigQueryTableCheckOperator (applied) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 17: Unexpected SQL injection found in BigQueryTableCheckOperator (applied)"
        failed=$((failed + 1))
    fi

    # Task 18: BigQueryTableCheckOperator (skipped) – same safety expectations
    if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "bq_table_check_skipped" "SET @@reservation" "false"; then
        log_info "✅ Task 18: BigQueryTableCheckOperator (skipped) is safe (no SQL injection)"
        passed=$((passed + 1))
    else
        log_error "❌ Task 18: Unexpected SQL injection found in BigQueryTableCheckOperator (skipped)"
        failed=$((failed + 1))
    fi

    log_info "=========================================="
    log_info "Test Results: ${passed}/18 passed, ${failed}/18 failed"
    log_info "=========================================="

    return $failed
}
