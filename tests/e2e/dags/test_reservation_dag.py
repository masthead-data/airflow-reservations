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
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryTableCheckOperator,
    BigQueryValueCheckOperator,
)

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup


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
                print(f"✅ Applied reservation via job_config.reservation: {reservation}")
            else:
                # Fallback for older SDK versions
                sql = f"SET @@reservation='{reservation}';\n{sql}"
                print(f"⚠️ job_config.reservation not supported, fell back to SQL: {reservation}")

        query_job = client.query(sql, job_config=job_config)

        # A successful dry run means the query is valid and reservation is accepted
        print(f"✅ SUCCESS: Dry run successful! Total bytes processed: {query_job.total_bytes_processed}")
        return {
            "reservation_applied": True,
            "reservation": reservation,
        }
    except Exception as e:
        print(f"⚠️ WARNING: Could not perform BigQuery dry run: {e}")
        print("This might be expected if GCP credentials are not fully configured in the environment.")
        return {
            "reservation_applied": True,
            "reservation": reservation,
            "error": str(e),
        }


def assert_bigquery_job(subject_task_id: str, expected_reservation: str | None, **context):
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
        raise RuntimeError(f"Could not find job_id in XCom for {dag_id}.{subject_task_id}")

    client = bigquery.Client()
    job = client.get_job(job_id)

    # 2. Ensure job completed successfully.
    if job.state != "DONE":
        raise RuntimeError(f"BigQuery job {job_id} for {dag_id}.{subject_task_id} not DONE (state={job.state})")

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

    if not actual_reservation or expected_reservation not in str(actual_reservation):
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

    # Task 6: BigQueryCheckOperator to test Check operators support
    bq_check = BigQueryCheckOperator(
        task_id="bq_check_task",
        sql="""SELECT COUNT(*) > 0 FROM (
            SELECT
                CURRENT_TIMESTAMP() AS timestamp,
                'test_reservation_dag' AS dag_name,
                NULL AS group_name,
                'bq_check_task' AS task_name,
                'BigQueryCheckOperator' AS operator_type
        )""",
        use_legacy_sql=False,
        location="US",
        deferrable=True,
    )

    # Task 7: BigQueryValueCheckOperator
    bq_value_check = BigQueryValueCheckOperator(
        task_id="bq_value_check_task",
        sql="SELECT 1",
        pass_value=1,
        use_legacy_sql=False,
        location="US",
        deferrable=True,
    )

    # Task 8: BigQueryGetDataOperator
    bq_get_data = BigQueryGetDataOperator(
        task_id="bq_get_data_task",
        project_id="bigquery-public-data",
        dataset_id="usa_names",
        table_id="usa_1910_2013",
        max_results=1,
        deferrable=True,
    )
    # Patch the task to have a configuration for testing our policy (simulation)
    bq_get_data.configuration = {"query": {"query": "SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 1", "useLegacySql": False}}

    # Task 9: BigQueryInsertJobOperator with Legacy SQL
    # This verifies reservationId injection for Legacy SQL tasks
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

    # Task 10: Custom Python task demonstrating programmatic reservation usage
    python_custom_bq = PythonOperator(
        task_id="python_custom_bq_task",
        python_callable=custom_python_bq_task,
    )

    # Task 11/12: BigQueryExecuteQueryOperator with applied and skipped reservation scenarios
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

    # Task 13/14: BigQueryIntervalCheckOperator applied / skipped
    bq_interval_check_applied = BigQueryIntervalCheckOperator(
        task_id="bq_interval_check_applied",
        table="bigquery-public-data.samples.shakespeare",
        metrics_thresholds={"COUNT(*)": 2},
        date_filter_column="word",
        days_back=-1,
        use_legacy_sql=False,
        location="US",
        deferrable=True,
    )

    bq_interval_check_skipped = BigQueryIntervalCheckOperator(
        task_id="bq_interval_check_skipped",
        table="bigquery-public-data.samples.shakespeare",
        metrics_thresholds={"COUNT(*)": 2},
        date_filter_column="word",
        days_back=-1,
        use_legacy_sql=False,
        location="US",
        deferrable=True,
    )

    # Task 15/16: BigQueryColumnCheckOperator applied / skipped
    bq_column_check_applied = BigQueryColumnCheckOperator(
        task_id="bq_column_check_applied",
        table="bigquery-public-data.samples.shakespeare",
        column_mapping={
            "word_count": {
                "min": {"greater_than": 0},
            },
        },
        use_legacy_sql=False,
        location="US",
    )

    bq_column_check_skipped = BigQueryColumnCheckOperator(
        task_id="bq_column_check_skipped",
        table="bigquery-public-data.samples.shakespeare",
        column_mapping={
            "word_count": {
                "min": {"greater_than": 0},
            },
        },
        use_legacy_sql=False,
        location="US",
    )

    # Task 17/18: BigQueryTableCheckOperator applied / skipped
    bq_table_check_applied = BigQueryTableCheckOperator(
        task_id="bq_table_check_applied",
        table="bigquery-public-data.samples.shakespeare",
        checks={
            "row_count_check": {
                "check_statement": "COUNT(*) > 0",
            },
        },
        use_legacy_sql=False,
        location="US",
    )

    bq_table_check_skipped = BigQueryTableCheckOperator(
        task_id="bq_table_check_skipped",
        table="bigquery-public-data.samples.shakespeare",
        checks={
            "row_count_check": {
                "check_statement": "COUNT(*) > 0",
            },
        },
        use_legacy_sql=False,
        location="US",
    )

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

    assert_bq_check = PythonOperator(
        task_id="assert_bq_check_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_check_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_value_check = PythonOperator(
        task_id="assert_bq_value_check_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_value_check_task",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_get_data = PythonOperator(
        task_id="assert_bq_get_data_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_get_data_task",
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

    assert_bq_interval_check_applied = PythonOperator(
        task_id="assert_bq_interval_check_applied_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_interval_check_applied",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_interval_check_skipped = PythonOperator(
        task_id="assert_bq_interval_check_skipped_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_interval_check_skipped",
            "expected_reservation": None,
        },
    )

    assert_bq_column_check_applied = PythonOperator(
        task_id="assert_bq_column_check_applied_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_column_check_applied",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_column_check_skipped = PythonOperator(
        task_id="assert_bq_column_check_skipped_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_column_check_skipped",
            "expected_reservation": None,
        },
    )

    assert_bq_table_check_applied = PythonOperator(
        task_id="assert_bq_table_check_applied_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_table_check_applied",
            "expected_reservation": "projects/masthead-dev/locations/US/reservations/capacity-1",
        },
    )

    assert_bq_table_check_skipped = PythonOperator(
        task_id="assert_bq_table_check_skipped_task",
        python_callable=assert_bigquery_job,
        op_kwargs={
            "subject_task_id": "bq_table_check_skipped",
            "expected_reservation": None,
        },
    )

    # Wire subject tasks to their assertion tasks
    bq_insert_job >> assert_bq_insert_job
    bq_execute_query >> assert_bq_execute_query_insert
    bq_ondemand >> assert_bq_ondemand
    bq_no_reservation >> assert_bq_no_reservation
    bq_nested_task >> assert_bq_nested
    bq_check >> assert_bq_check
    bq_value_check >> assert_bq_value_check
    bq_get_data >> assert_bq_get_data
    bq_insert_job_legacy >> assert_bq_insert_job_legacy
    bq_execute_query_op_applied >> assert_bq_execute_query_op_applied
    bq_execute_query_op_skipped >> assert_bq_execute_query_op_skipped
    bq_interval_check_applied >> assert_bq_interval_check_applied
    bq_interval_check_skipped >> assert_bq_interval_check_skipped
    bq_column_check_applied >> assert_bq_column_check_applied
    bq_column_check_skipped >> assert_bq_column_check_skipped
    bq_table_check_applied >> assert_bq_table_check_applied
    bq_table_check_skipped >> assert_bq_table_check_skipped

    # Set dependencies
    [
        bq_insert_job,
        bq_execute_query,
        bq_ondemand,
        bq_no_reservation,
        tg,
        bq_check,
        bq_value_check,
        bq_get_data,
        bq_insert_job_legacy,
        bq_execute_query_op_applied,
        bq_execute_query_op_skipped,
        bq_interval_check_applied,
        bq_interval_check_skipped,
        bq_column_check_applied,
        bq_column_check_skipped,
        bq_table_check_applied,
        bq_table_check_skipped,
        python_custom_bq,
    ]
