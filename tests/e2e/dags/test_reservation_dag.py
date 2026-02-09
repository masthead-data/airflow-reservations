"""E2E Test DAG for airflow-reservations using real BigQuery operators.

This DAG tests the reservation policy with actual BigQuery operators.
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

# BigQueryExecuteQueryOperator is only available in provider versions 2.0.0 - 10.26.0
BigQueryExecuteQueryOperator = None
try:
    from importlib.metadata import version, PackageNotFoundError

    provider_version = version("apache-airflow-providers-google")
    version_parts = tuple(int(x) for x in provider_version.split(".")[:3])

    if (2, 0, 0) <= version_parts <= (10, 26, 0):
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryExecuteQueryOperator,
        )

        print(
            f"BigQueryExecuteQueryOperator available (provider version {provider_version})"
        )
    else:
        print(
            f"BigQueryExecuteQueryOperator not supported in provider version {provider_version}"
        )
except (ImportError, PackageNotFoundError, ValueError) as e:
    raise RuntimeError(
        f"Could not determine provider version or import BigQueryExecuteQueryOperator: {e}"
    ) from e

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup


def custom_python_bq_task(**context):
    """Custom Python task that executes a BigQuery job."""
    from airflow_reservations.config import get_reservation

    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    # Get the reservation for this task
    reservation = get_reservation(dag_id, task_id)

    print("=" * 60)
    print("Custom Python BigQuery Task")
    print("=" * 60)

    if not reservation:
        print(
            "⚠️ WARNING: No reservation configured for this task, continuing without it."
        )

    sql = """SELECT
    CURRENT_TIMESTAMP() AS timestamp,
    'test_reservation_dag' AS dag_name,
    NULL AS group_name,
    'test_python_custom_operator_applied' AS task_name,
    'PythonOperator' AS operator_type"""

    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(use_query_cache=False)

        if reservation:
            if hasattr(job_config, "reservation"):
                job_config.reservation = reservation
                print(
                    f"✅ Applied reservation via job_config.reservation: {reservation}"
                )
            else:
                sql = f"SET @@reservation='{reservation}';\n{sql}"
                print(
                    f"⚠️ job_config.reservation not supported, fell back to SQL: {reservation}"
                )

            # Log in a format that common.sh can detect
            print(f"INFO - Applied reservation: {{'reservation': '{reservation}'}}")

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        # Return the job_id so that the assertion task can fetch it
        return query_job.job_id
    except Exception as e:
        print(f"⚠️ WARNING: Could not perform BigQuery job: {e}")
        return {
            "error": str(e),
        }


def assert_bigquery_job(subject_task_id: str, **context):
    """Assertion task that validates the upstream BigQuery job."""
    from google.cloud import bigquery

    from airflow_reservations.config import get_reservation

    ti = context["ti"]
    dag_id = context["dag"].dag_id

    expected_reservation = get_reservation(dag_id, subject_task_id)
    print(
        f"Checking task {subject_task_id} against expected reservation: {expected_reservation}"
    )

    # Resolve job_id from XCom.
    job_id = ti.xcom_pull(task_ids=subject_task_id, key="job_id")
    if not job_id:
        job_id = ti.xcom_pull(task_ids=subject_task_id)
    if not job_id:
        raise RuntimeError(
            f"Could not find job_id in XCom for {dag_id}.{subject_task_id}"
        )

    client = bigquery.Client()
    job = client.get_job(job_id)

    if job.state != "DONE":
        raise RuntimeError(
            f"BigQuery job {job_id} for {dag_id}.{subject_task_id} not DONE (state={job.state})"
        )

    if getattr(job, "error_result", None):
        raise RuntimeError(
            f"BigQuery job {job_id} for {dag_id}.{subject_task_id} failed: {job.error_result}"
        )

    if not expected_reservation or expected_reservation == "none":
        return

    actual_reservation = None
    reservation_usage = getattr(job, "reservation_usage", None)
    if reservation_usage:
        try:
            actual_reservation = reservation_usage[0].name
        except Exception:
            actual_reservation = None

    if not actual_reservation:
        stats = getattr(job, "_properties", {}).get("statistics", {})
        actual_reservation = stats.get("reservation_id")

    if actual_reservation:
        is_match = expected_reservation in actual_reservation
        if not is_match:
            exp_name = expected_reservation.split("/")[-1]
            act_name = (
                str(actual_reservation)
                .replace(".", "/")
                .replace(":", "/")
                .split("/")[-1]
            )
            is_match = exp_name == act_name

        if not is_match:
            raise AssertionError(
                f"Expected BigQuery job {job_id} for {dag_id}.{subject_task_id} to use reservation containing '{expected_reservation}', but got {actual_reservation}"
            )
        print(f"✅ Job {job_id} correctly used reservation: {actual_reservation}")


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
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "reservations", "e2e"],
) as dag:

    # BigQueryInsertJobOperator
    test_bq_insert_job_std_sql_applied = BigQueryInsertJobOperator(
        task_id="test_bq_insert_job_std_sql_applied",
        configuration={
            "query": {
                "query": "SELECT 1 as val",
                "useQueryCache": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # BigQueryExecuteQueryOperator
    if BigQueryExecuteQueryOperator:
        test_bq_execute_query_std_sql_applied = BigQueryExecuteQueryOperator(
            task_id="test_bq_execute_query_std_sql_applied",
            sql="SELECT 'execute-query' as val",
            api_resource_configs={"query": {"useQueryCache": False}},
            location="US",
        )

        test_bq_execute_query_manual_res_applied = BigQueryExecuteQueryOperator(
            task_id="test_bq_execute_query_manual_res_applied",
            sql="SELECT 'manual-res' as val",
            api_resource_configs={
                "reservation": "projects/p/locations/US/reservations/r",
                "query": {"useQueryCache": False},
            },
            location="US",
        )
    else:
        test_bq_execute_query_std_sql_applied = None
        test_bq_execute_query_manual_res_applied = None

    # BigQueryInsertJobOperator - Nested in TaskGroup
    with TaskGroup("tg_group") as tg:
        test_bq_insert_job_nested_applied = BigQueryInsertJobOperator(
            task_id="test_bq_insert_job_nested_applied",
            configuration={
                "query": {
                    "query": "SELECT 'nested' as val",
                    "useQueryCache": False,
                },
            },
            project_id="masthead-dev",
            location="US",
        )

    # PythonOperator - Custom BigQuery Client
    test_python_custom_operator_applied = PythonOperator(
        task_id="test_python_custom_operator_applied",
        python_callable=custom_python_bq_task,
    )

    # BigQueryInsertJobOperator - Explicit On-demand
    test_bq_insert_job_on_demand_skipped = BigQueryInsertJobOperator(
        task_id="test_bq_insert_job_on_demand_skipped",
        configuration={
            "query": {
                "query": "SELECT 'on-demand' as val",
                "useQueryCache": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # BigQueryInsertJobOperator - No Reservation (Explicitly mapped to null)
    test_bq_insert_job_no_reservation_null = BigQueryInsertJobOperator(
        task_id="test_bq_insert_job_no_reservation_null",
        configuration={
            "query": {
                "query": "SELECT 'no-res' as val",
                "useQueryCache": False,
            },
        },
        project_id="masthead-dev",
        location="US",
    )

    # Assertion tasks
    def create_assert(subject_id):
        return PythonOperator(
            task_id=f"assert_{subject_id.replace('.', '_')}",
            python_callable=assert_bigquery_job,
            op_kwargs={"subject_task_id": subject_id},
        )

    (
        create_assert("test_bq_insert_job_std_sql_applied")
        << test_bq_insert_job_std_sql_applied
    )
    (
        create_assert("test_bq_insert_job_on_demand_skipped")
        << test_bq_insert_job_on_demand_skipped
    )
    (
        create_assert("test_bq_insert_job_no_reservation_null")
        << test_bq_insert_job_no_reservation_null
    )
    (
        create_assert("tg_group.test_bq_insert_job_nested_applied")
        << test_bq_insert_job_nested_applied
    )

    (
        create_assert("test_python_custom_operator_applied")
        << test_python_custom_operator_applied
    )

    if test_bq_execute_query_std_sql_applied:
        (
            create_assert("test_bq_execute_query_std_sql_applied")
            << test_bq_execute_query_std_sql_applied
        )
        (
            create_assert("test_bq_execute_query_manual_res_applied")
            << test_bq_execute_query_manual_res_applied
        )
