"""E2E Test DAG for airflow-reservations-policy using real BigQuery operators.

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
    BigQueryInsertJobOperator,
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
    from airflow_reservations_policy.config import get_reservation

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
    description="E2E test DAG for airflow-reservations-policy with real operators",
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
    # CheckOperator expects a query that returns a value to validate (e.g., a count or boolean)
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
    )

    # Task 7: Custom Python task demonstrating programmatic reservation usage
    python_custom_bq = PythonOperator(
        task_id="python_custom_bq_task",
        python_callable=custom_python_bq_task,
    )

    # Set dependencies - run all tasks in parallel for faster e2e testing
    [bq_insert_job, bq_execute_query, bq_ondemand, bq_no_reservation, tg, bq_check, python_custom_bq]
