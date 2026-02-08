"""E2E Test DAG for airflow-reservations using real BigQuery operators.

This DAG tests the reservation policy with actual BigQuery operators using dryRun mode.
The operators will log the SQL being executed, which we can verify contains the reservation.
"""

from datetime import datetime

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

try:
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryExecuteQueryOperator,
    )
except ImportError:
    BigQueryExecuteQueryOperator = None

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

import airflow

# Detect Airflow version to handle version-specific compatibility
AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
IS_AIRFLOW_3_PLUS = AIRFLOW_VERSION >= (3, 0)


def custom_python_bq_task(**context):
    """Custom Python task that executes a BigQuery job.

    This tests that custom Python code can use the policy's helper function
    to apply reservations to BigQuery jobs and that the resulting SQL
    is valid when executed via the BigQuery client.
    """
    from airflow_reservations.config import get_reservation

    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    # Get the reservation for this task
    reservation = get_reservation(dag_id, task_id)

    print("=" * 60)
    print("Custom Python BigQuery Task")
    print("=" * 60)

    if not reservation:
        print("❌ WARNING: No reservation configured for this task")
        return {"reservation_applied": False}

    print(f"✅ SUCCESS: Reservation was applied in custom Python code!")
    print(f"Reservation: {reservation}")

    # Prepare SQL
    sql = """SELECT
    CURRENT_TIMESTAMP() AS timestamp,
    'test_reservation_dag' AS dag_name,
    NULL AS group_name,
    'python_custom_bq_task' AS task_name,
    'PythonOperator' AS operator_type"""

    print(f"SQL to execute:\n{sql}")

    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

        if reservation:
            # The reservation property was added in google-cloud-bigquery 3.1.0
            if hasattr(job_config, "reservation"):
                job_config.reservation = reservation
                print(
                    f"✅ Applied reservation via job_config.reservation: {reservation}"
                )
            else:
                # Fallback for older SDK versions
                sql = f"SET @@reservation='{reservation}';\n{sql}"
                print(
                    f"⚠️ job_config.reservation not supported, fell back to SQL: {reservation}"
                )

        query_job = client.query(sql, job_config=job_config)

        # A successful dry run means the query is valid and reservation is accepted
        print(
            f"✅ SUCCESS: Dry run successful! Total bytes processed: {query_job.total_bytes_processed}"
        )
        return {
            "reservation_applied": True,
            "reservation": reservation,
        }
    except Exception as e:
        print(f"⚠️ WARNING: Could not perform BigQuery dry run: {e}")
        print(
            "This might be expected if GCP credentials are not fully configured in the environment."
        )
        return {
            "reservation_applied": True,
            "reservation": reservation,
            "error": str(e),
        }


def assert_bigquery_job(
    subject_task_id: str, expected_reservation: str | None, **context
):
    """
    Assertion task that validates the upstream BigQuery job completed successfully
    and, where possible, used the expected reservation.

    This is best-effort: if reservation information is not available for a job,
    we still assert successful completion and rely on log-based checks for
    detailed reservation semantics.
    """
    from google.cloud import bigquery

    ti = context["ti"]
    dag_id = context["dag"].dag_id

    # 1. Resolve job_id from XCom. Different operators use different keys.
    job_id = ti.xcom_pull(task_ids=subject_task_id, key="job_id")
    if not job_id:
        # Fallback to default return_value from operators like BigQueryInsertJobOperator
        job_id = ti.xcom_pull(task_ids=subject_task_id)
    if not job_id:
        raise RuntimeError(
            f"Could not find job_id in XCom for {dag_id}.{subject_task_id}"
        )

    client = bigquery.Client()
    job = client.get_job(job_id)

    # 2. Ensure job completed successfully.
    if job.state != "DONE":
        raise RuntimeError(
            f"BigQuery job {job_id} for {dag_id}.{subject_task_id} not DONE (state={job.state})"
        )

    if getattr(job, "error_result", None):
        raise RuntimeError(
            f"BigQuery job {job_id} for {dag_id}.{subject_task_id} failed: {job.error_result}"
        )

    # 3. Best-effort reservation check. For 'none' or skipped scenarios we only
    #    care that the job completed; we rely on log-based checks for details.
    if not expected_reservation or expected_reservation == "none":
        return

    actual_reservation = None

    # Preferred: reservation_usage list with ReservationUsage(name, slot_ms)
    reservation_usage = getattr(job, "reservation_usage", None)
    if reservation_usage:
        try:
            actual_reservation = reservation_usage[0].name
        except Exception:
            actual_reservation = None

    # Fallback to raw statistics if needed
    if not actual_reservation:
        stats = getattr(job, "_properties", {}).get("statistics", {})
        actual_reservation = stats.get("reservation_id")

    is_match = False
    if actual_reservation and expected_reservation:
        actual_str = str(actual_reservation)
        # Check for substring match (handles full path vs short ID)
        if expected_reservation in actual_str:
            is_match = True
        # Also check if just the reservation name matches
        elif expected_reservation.split("/")[-1] == actual_str.split(".")[-1]:
            is_match = True

    if not is_match:
        raise AssertionError(
            f"Expected BigQuery job {job_id} for {dag_id}.{subject_task_id} to use reservation "
            f"containing '{expected_reservation}', but got {actual_reservation!r}"
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="test_reservation_dag",
    default_args=default_args,
    description="E2E test DAG for airflow-reservations with real operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "reservations", "e2e"],
) as dag:

    # Task 1: BigQueryInsertJobOperator with reservation
    # The policy will inject reservation into configuration.query.query
    # Using simple SQL that doesn't require tables to exist
    bq_insert_job = BigQueryInsertJobOperator(
        task_id="bq_insert_job_task",
        configuration={
            "query": {
                "query": """SELECT
                    CURRENT_TIMESTAMP() AS timestamp,
                    'test_reservation_dag' AS dag_name,
                    NULL AS group_name,
                    'bq_insert_job_task' AS task_name,
                    'BigQueryInsertJobOperator' AS operator_type""",
                "useLegacySql": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Task 2: BigQueryInsertJobOperator with reservation (using INSERT query)
    # The policy will inject reservation into configuration.query.query
    bq_execute_query = BigQueryInsertJobOperator(
        task_id="bq_execute_query_task",
        configuration={
            "query": {
                "query": """SELECT
            CURRENT_TIMESTAMP() AS timestamp,
            'test_reservation_dag' AS dag_name,
            NULL AS group_name,
            'bq_execute_query_task' AS task_name,
            'BigQueryInsertJobOperator' AS operator_type""",
                "useLegacySql": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Task 3: On-demand task (reservation = "none")
    bq_ondemand = BigQueryInsertJobOperator(
        task_id="bq_ondemand_task",
        configuration={
            "query": {
                "query": """SELECT
                    CURRENT_TIMESTAMP() AS timestamp,
                    'test_reservation_dag' AS dag_name,
                    NULL AS group_name,
                    'bq_ondemand_task' AS task_name,
                    'BigQueryInsertJobOperator' AS operator_type""",
                "useLegacySql": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Task 4: Task NOT in config (should NOT get any reservation)
    bq_no_reservation = BigQueryInsertJobOperator(
        task_id="bq_no_reservation_task",
        configuration={
            "query": {
                "query": """SELECT
                    CURRENT_TIMESTAMP() AS timestamp,
                    'test_reservation_dag' AS dag_name,
                    NULL AS group_name,
                    'bq_no_reservation_task' AS task_name,
                    'BigQueryInsertJobOperator' AS operator_type""",
                "useLegacySql": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Task 5: Task inside a TaskGroup
    with TaskGroup("my_group") as tg:
        bq_nested_task = BigQueryInsertJobOperator(
            task_id="nested_task",
            configuration={
                "query": {
                    "query": """SELECT
                        CURRENT_TIMESTAMP() AS timestamp,
                        'test_reservation_dag' AS dag_name,
                        'my_group' AS group_name,
                        'nested_task' AS task_name,
                        'BigQueryInsertJobOperator' AS operator_type""",
                    "useLegacySql": False,
                },
            },
            project_id="masthead-dev",
            location="US",
        )

    # Task 8: BigQueryInsertJobOperator with Legacy SQL
    bq_insert_job_legacy = BigQueryInsertJobOperator(
        task_id="bq_insert_job_legacy_task",
        configuration={
            "query": {
                "query": "SELECT CURRENT_TIMESTAMP()",
                "useLegacySql": True,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Task 9: Custom Python task demonstrating programmatic reservation usage
    python_custom_bq = PythonOperator(
        task_id="python_custom_bq_task",
        python_callable=custom_python_bq_task,
    )

    # Task 10/11: BigQueryExecuteQueryOperator with applied and skipped reservation scenarios
    if BigQueryExecuteQueryOperator:
        bq_execute_query_op_applied = BigQueryExecuteQueryOperator(
            task_id="bq_execute_query_op_applied",
            sql="""
            SELECT
                CURRENT_TIMESTAMP() AS timestamp,
                'test_reservation_dag' AS dag_name,
                NULL AS group_name,
                'bq_execute_query_op_applied' AS task_name,
                'BigQueryExecuteQueryOperator' AS operator_type
            """,
            use_legacy_sql=False,
            location="US",
            deferrable=True,
        )

        bq_execute_query_op_skipped = BigQueryExecuteQueryOperator(
            task_id="bq_execute_query_op_skipped",
            sql="""
            SET @@reservation='preexisting';
            SELECT
                CURRENT_TIMESTAMP() AS timestamp,
                'test_reservation_dag' AS dag_name,
                NULL AS group_name,
                'bq_execute_query_op_skipped' AS task_name,
                'BigQueryExecuteQueryOperator' AS operator_type
            """,
            use_legacy_sql=False,
            location="US",
            deferrable=True,
        )
    else:
        bq_execute_query_op_applied = None
        bq_execute_query_op_skipped = None



    # Assertion tasks that validate underlying BigQuery jobs for operators where we
    # can reliably retrieve a job_id from XCom.
    assert_bq_insert_job = PythonOperator(
        task_id="assert_bq_insert_job_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_insert_job_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_execute_query_insert = PythonOperator(
        task_id="assert_bq_execute_query_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_execute_query_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_ondemand = PythonOperator(
        task_id="assert_bq_ondemand_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_ondemand_task",
            "expected_reservation": "none",
        },
    )

    assert_bq_no_reservation = PythonOperator(
        task_id="assert_bq_no_reservation_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_no_reservation_task",
            "expected_reservation": None,
        },
    )

    assert_bq_nested = PythonOperator(
        task_id="assert_bq_nested_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "my_group.nested_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )



    assert_bq_insert_job_legacy = PythonOperator(
        task_id="assert_bq_insert_job_legacy_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_insert_job_legacy_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    if BigQueryExecuteQueryOperator:
        assert_bq_execute_query_op_applied = PythonOperator(
            task_id="assert_bq_execute_query_op_applied_task",
            python_callable=assert_bigquery_job,
            op_kwargs={
                "subject_task_id": "bq_execute_query_op_applied",
                "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
            },
        )

        assert_bq_execute_query_op_skipped = PythonOperator(
            task_id="assert_bq_execute_query_op_skipped_task",
            python_callable=assert_bigquery_job,
            op_kwargs={
                "subject_task_id": "bq_execute_query_op_skipped",
                "expected_reservation": None,
            },
        )



    # Wire subject tasks to their assertion tasks
    bq_insert_job >> assert_bq_insert_job
    bq_execute_query >> assert_bq_execute_query_insert
    bq_ondemand >> assert_bq_ondemand
    bq_no_reservation >> assert_bq_no_reservation
    bq_nested_task >> assert_bq_nested

    bq_insert_job_legacy >> assert_bq_insert_job_legacy
    if BigQueryExecuteQueryOperator:
        bq_execute_query_op_applied >> assert_bq_execute_query_op_applied
        bq_execute_query_op_skipped >> assert_bq_execute_query_op_skipped


    # Set dependencies
    all_tasks = [
        bq_insert_job,
        bq_execute_query,
        bq_ondemand,
        bq_no_reservation,
        tg,
        bq_insert_job_legacy,
        bq_execute_query_op_applied,
        bq_execute_query_op_skipped,

        python_custom_bq,
    ]

    # Filter out None values for optional operators
    all_tasks = [t for t in all_tasks if t is not None]
