"""E2E Test DAG for airflow-reservations-policy using real BigQuery operators.

This DAG tests the reservation policy with actual BigQuery operators using dryRun mode.
The operators will log the SQL being executed, which we can verify contains the reservation.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup


def custom_python_bq_task(**context):
    """Custom Python task that executes a BigQuery job.

    This tests that custom Python code can use the policy's helper function
    to apply reservations to BigQuery jobs.
    """
    # Import here to avoid parse-time import errors
    try:
        from google.cloud import bigquery
    except ImportError:
        print("google-cloud-bigquery not installed, skipping BQ client demo")

    from airflow_reservations_policy.config import get_reservation

    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id

    # Get the reservation for this task
    reservation = get_reservation(dag_id, task_id)

    print("=" * 60)
    print("Custom Python BigQuery Task")
    print("=" * 60)

    if reservation:
        print(f"✅ SUCCESS: Reservation was applied in custom Python code!")
        print(f"Reservation: {reservation}")

        # Demonstrate how to use it with BigQuery client
        sql_with_reservation = f"""SET @@reservation_id = '{reservation}';
SELECT
    CURRENT_TIMESTAMP() AS timestamp,
    'test_reservation_dag' AS dag_name,
    NULL AS group_name,
    'python_custom_bq_task' AS task_name,
    'PythonOperator' AS operator_type"""
        print(f"SQL with reservation:\n{sql_with_reservation}")

        return {"reservation_applied": True, "reservation": reservation}
    else:
        print("❌ WARNING: No reservation configured for this task")
        return {"reservation_applied": False}


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
            "dryRun": True,
        },
        project_id="masthead-prod",
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
            "dryRun": True,
        },
        project_id="masthead-prod",
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
            "dryRun": True,
        },
        project_id="masthead-prod",
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
            "dryRun": True,
        },
        project_id="masthead-prod",
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
                "dryRun": True,
            },
            project_id="masthead-prod",
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
        project_id="masthead-prod",
        location="US",
    )

    # Task 7: Custom Python task demonstrating programmatic reservation usage
    python_custom_bq = PythonOperator(
        task_id="python_custom_bq_task",
        python_callable=custom_python_bq_task,
    )

    # Set dependencies
    bq_insert_job >> bq_execute_query >> bq_ondemand >> bq_no_reservation >> tg >> bq_check >> python_custom_bq
