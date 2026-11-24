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
    from importlib.metadata import PackageNotFoundError, version

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

    ti = context["task_instance"]

    # Get the reservation for this task
    reservation = get_reservation(ti.dag_id, ti.task_id)

    print("=" * 60)
    print("Custom Python BigQuery Task")
    print("=" * 60)

    if not reservation:
        print(
            "⚠️ WARNING: No reservation configured for this task, continuing without it."
        )

    sql = f"""SELECT
    CURRENT_TIMESTAMP() AS timestamp,
    '{ti.dag_id}' AS dag_name,
    '{ti.task_id}' AS task_name,
    'PythonOperator' AS operator_type"""

    from google.cloud import bigquery

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=False)

    if reservation:
        # Setting reservation via _properties to support older versions of
        # google-cloud-bigquery that don't have it as a first-class property
        job_config._properties["reservation"] = reservation
        print(f"✅ Applied reservation: {reservation}")

        # Log in a format that common.sh can detect
        print(f"INFO - Applied reservation: {{'reservation': '{reservation}'}}")

    # Let BigQuery errors fail the task instead of hiding them from the assertion
    query_job = client.query(sql, job_config=job_config)
    query_job.result()

    # Return the job_id so that the assertion task can fetch it
    return query_job.job_id


_UNCONFIGURED = object()


def _expected_reservation(dag_id: str, task_id: str):
    """Read the expected reservation straight from the test config file.

    Deliberately does not use airflow_reservations, so the assertion does not
    trust the code under test. Returns the reservation string ("none" included),
    None for an explicit null, or _UNCONFIGURED when the task is not listed.
    """
    import json
    import os

    config_path = os.path.join(os.path.dirname(__file__), "reservations_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    task_key = f"{dag_id}.{task_id.replace('.', '-')}"
    for entry in config["reservation_config"]:
        if task_key in entry["tasks"]:
            return entry["reservation"]
    return _UNCONFIGURED


def _reservation_id_from_path(path: str) -> str:
    """Convert projects/P/locations/L/reservations/R to BigQuery's P:L.R form."""
    parts = path.split("/")
    if len(parts) != 6 or parts[0::2] != ["projects", "locations", "reservations"]:
        raise ValueError(f"Unexpected reservation path in test config: {path}")
    return f"{parts[1]}:{parts[3]}.{parts[5]}"


def assert_bigquery_job(subject_task_id: str, **context):
    """Assert the reservation that the upstream BigQuery job actually ran on."""
    from google.cloud import bigquery

    ti = context["task_instance"]

    expected_reservation = _expected_reservation(ti.dag_id, subject_task_id)
    if expected_reservation is _UNCONFIGURED:
        raise RuntimeError(
            f"Task {subject_task_id} has an assertion but is not in reservations_config.json"
        )
    print(
        f"Checking task {subject_task_id} against expected reservation: {expected_reservation}"
    )

    # Resolve job_id from XCom.
    job_id = ti.xcom_pull(task_ids=subject_task_id, key="job_id")
    if not job_id:
        job_id = ti.xcom_pull(task_ids=subject_task_id)
    if not job_id:
        raise RuntimeError(
            f"Could not find job_id in XCom for {ti.dag_id}.{subject_task_id}"
        )

    client = bigquery.Client()
    job = client.get_job(job_id)

    if job.state != "DONE":
        raise RuntimeError(
            f"BigQuery job {job_id} for {ti.dag_id}.{subject_task_id} not DONE (state={job.state})"
        )

    if getattr(job, "error_result", None):
        raise RuntimeError(
            f"BigQuery job {job_id} for {ti.dag_id}.{subject_task_id} failed: {job.error_result}"
        )

    # configuration.reservation is what the job requested;
    # statistics.reservation_id is where BigQuery actually ran it (P:L.R, empty
    # for on-demand).
    requested = job._properties.get("configuration", {}).get("reservation")
    actual = job._properties.get("statistics", {}).get("reservation_id")
    print(f"Job {job_id}: requested={requested!r}, ran on={actual!r}")

    subject = f"BigQuery job {job_id} for {ti.dag_id}.{subject_task_id}"
    if expected_reservation is None:
        # null: the package must leave the job alone, so it runs on the
        # project's default assignment, whatever that is.
        if requested is not None:
            raise AssertionError(
                f"{subject} should not request a reservation, but requested {requested!r}"
            )
    elif expected_reservation == "none":
        if requested != "none" or actual:
            raise AssertionError(
                f"{subject} should run on-demand (requested 'none', no reservation), "
                f"but requested {requested!r} and ran on {actual!r}"
            )
    else:
        expected_id = _reservation_id_from_path(expected_reservation)
        if requested != expected_reservation or actual != expected_id:
            raise AssertionError(
                f"{subject} should request {expected_reservation!r} and run on "
                f"{expected_id!r}, but requested {requested!r} and ran on {actual!r}"
            )
    print(f"✅ {subject} matches the expected reservation")


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
                "query": "SELECT 'test_bq_insert_job_std_sql_applied' as task_name",
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
            sql="SELECT 'test_bq_execute_query_std_sql_applied' as task_name",
            api_resource_configs={"query": {"useQueryCache": False}},
            location="US",
        )

        test_bq_execute_query_manual_res_applied = BigQueryExecuteQueryOperator(
            task_id="test_bq_execute_query_manual_res_applied",
            sql="SELECT 'test_bq_execute_query_manual_res_applied' as task_name",
            api_resource_configs={
                "reservation": "projects/p/locations/US/reservations/r",
                "query": {"useQueryCache": False},
            },
            location="US",
        )
    else:
        test_bq_execute_query_std_sql_applied = None
        test_bq_execute_query_manual_res_applied = None

    # TaskGroup examples
    with TaskGroup("tg_group") as tg:
        test_bq_insert_job_nested_applied = BigQueryInsertJobOperator(
            task_id="test_bq_insert_job_nested_applied",
            configuration={
                "query": {
                    "query": "SELECT 'test_bq_insert_job_nested_applied' as task_name",
                    "useQueryCache": False,
                },
            },
            project_id="masthead-dev",
            location="US",
        )

        # PythonOperator - TaskGroup path matching README-style example
        test_python_custom_operator_nested_applied = PythonOperator(
            task_id="test_python_custom_operator_nested_applied",
            python_callable=custom_python_bq_task,
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
                "query": "SELECT 'test_bq_insert_job_on_demand_skipped' as task_name",
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
                "query": "SELECT 'test_bq_insert_job_no_reservation_null' as task_name",
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
        create_assert("tg_group.test_python_custom_operator_nested_applied")
        << test_python_custom_operator_nested_applied
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
