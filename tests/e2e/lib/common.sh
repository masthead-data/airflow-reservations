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

    local args=""
    if [ "$airflow_version" = "2" ]; then
        args="-d \"$dag_id\""
    else
        args="\"$dag_id\""
    fi

    local cmd="airflow dags list-runs $args -o json"
    
    docker compose -f "$compose_file" exec -T "$container_name" sh -c "$cmd" 2>/dev/null | \
        grep -o '"run_id": "[^"]*"' | head -1 | cut -d'"' -f4
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
        echo "$logs" | grep -q "$search_pattern"
    else
        ! echo "$logs" | grep -q "$search_pattern"
    fi
}

# Run verification for all standard test tasks
# Args: $1 = compose_file, $2 = log_container, $3 = dag_id, $4 = run_id
# Returns: number of failed tests
# Parse configuration file
parse_config() {
    local config_file="$1"
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed."
        return 1
    fi
    
    # Extract mappings: task_id -> output_line (reservation|tag)
    # We strip the DAG ID prefix (assuming format dag_id.task_id) to match Airflow task IDs
    jq -r '.reservation_config[] | .reservation as $res | .tasks[] | sub("^[^.]*\\."; "") + "|" + $res' "$config_file"
}

# Run verification for all tasks based on configuration
# Args: $1 = compose_file, $2 = log_container, $3 = dag_id, $4 = run_id, $5 = config_file
# Returns: number of failed tests
verify_all_tasks() {
    local compose_file="$1"
    local log_container="$2"
    local dag_id="$3"
    local run_id="$4"
    local config_file="${5:-../dags/reservations_config.json}" # Default path if not provided
    local passed=0
    local failed=0

    # Locate config file relative to script if needed
    if [ ! -f "$config_file" ]; then
        # Try finding it relative to current script directory if the path is relative
        local script_dir=$(dirname "${BASH_SOURCE[0]}")
        local alt_config_file="$script_dir/$config_file"
        if [ -f "$alt_config_file" ]; then
            config_file="$alt_config_file"
        else
            log_error "Configuration file not found: $config_file"
            return 1
        fi
    fi

    log_info "=========================================="
    log_info "Verifying reservation injection..."
    log_info "Using config: $config_file"
    log_info "=========================================="

    # 1. Parse configuration into a map (task_id -> reservation)
    # Parsing into a string with newlines is easiest.
    local config_map
    if ! config_map=$(parse_config "$config_file"); then
         return 1
    fi
    
    # 2. Get list of tasks from Airflow
    local task_list_output
    # Try with explicit json output first (most reliable across versions if supported), then fallback to default
    if ! task_list_output=$(docker compose -f "$compose_file" exec -T "$log_container" airflow tasks list "$dag_id" --output text 2>/dev/null); then
         task_list_output=$(docker compose -f "$compose_file" exec -T "$log_container" airflow tasks list "$dag_id" 2>/dev/null)
    fi

    # Clean up output to get just task IDs (remove headers/logs if any)
    local tasks
    tasks=$(echo "$task_list_output" | grep -E "^[a-zA-Z0-9_.]+$" | sort)

    if [ -z "$tasks" ]; then
        log_error "Failed to retrieve task list for DAG $dag_id"
        return 1
    fi

    # 3. Verify all configured tasks exist in the DAG
    log_info "Checking for orphan configuration entries..."
    
    local configured_tasks
    configured_tasks=$(echo "$config_map" | cut -d'|' -f1 | sort | uniq | grep -v "^$")

    local missing_task_count=0
    local IFS=$'\n'
    for conf_task in $configured_tasks; do
        if ! echo "$tasks" | grep -q "^${conf_task}$"; then
            log_error "❌ Configured task '$conf_task' NOT found in DAG tasks list"
            missing_task_count=$((missing_task_count + 1))
        fi
    done
    unset IFS

    if [ $missing_task_count -gt 0 ]; then
        failed=$((failed + missing_task_count))
    else
        log_info "✅ All configured tasks are present in the DAG"
    fi

    local IFS=$'\n'
    for task_id in $tasks; do
        # Skip assertion tasks in shell verification to avoid false positives 
        # (they log their subject's configuration which contains reservation strings)
        if [[ "$task_id" == assert_* ]]; then
            continue
        fi

        # Skip verification if logs don't exist (task didn't run)
        local log_path="/opt/airflow/logs/dag_id=${dag_id}/run_id=${run_id}/task_id=${task_id}/attempt=1.log"
        if ! docker compose -f "$compose_file" exec -T "$log_container" test -f "$log_path" 2>/dev/null; then
            log_info "⏭️  Task $task_id: No logs found (skipped or not run). Skipping verification."
            continue
        fi

        # Lookup reservation from config_map
        local expected_reservation=""
        expected_reservation=$(echo "$config_map" | grep "^${task_id}|" | cut -d'|' -f2)
        
        if [ -n "$expected_reservation" ]; then
            # Task is configured
            # Expect specific reservation (including 'none')
            if verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "$task_id" "'reservation': '$expected_reservation'" "true" || \
               verify_task_log "$compose_file" "$log_container" "$dag_id" "$run_id" "$task_id" "SET @@reservation='$expected_reservation'" "true"; then
                log_info "✅ Task $task_id: Correctly has reservation '$expected_reservation'"
                passed=$((passed + 1))
            else
                log_error "❌ Task $task_id: Expected reservation '$expected_reservation' NOT found"
                failed=$((failed + 1))
            fi
        else
            # Task is NOT configured -> Expect NO injection
            local match_line
            match_line=$(docker compose -f "$compose_file" exec -T "$log_container" grep "Injected reservation .* into task ${task_id} " "$log_path" 2>/dev/null | head -n 1)
            if [ -z "$match_line" ]; then
                log_info "✅ Task $task_id: Correctly has no reservation"
                passed=$((passed + 1))
            else
                log_error "❌ Task $task_id: Unexpected reservation injection found"
                log_error "  Line found: $match_line"
                failed=$((failed + 1))
            fi
        fi
    done
    unset IFS

    log_info "=========================================="
    log_info "Test Results: ${passed} passed, ${failed} failed"
    log_info "=========================================="

    return $failed
}
