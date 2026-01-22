"""Tests for the policy module."""

from unittest import mock


class MockTask:
    """Mock Airflow task for testing."""

    def __init__(
        self,
        task_id: str,
        task_type: str,
        dag_id: str = "test_dag",
        configuration: dict = None,
        sql: str = None,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.dag_id = dag_id
        self.configuration = configuration
        self.sql = sql
        self.dag = None


class TestTaskPolicy:
    """Tests for the task_policy function."""

    def test_ignores_non_bigquery_operators(self):
        """Test that non-BigQuery operators are ignored."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="PythonOperator",
            configuration={"query": {"query": "SELECT 1"}},
        )

        with mock.patch.object(policy, "get_reservation", return_value="res1"):
            policy.task_policy(task)

        # Configuration should be unchanged
        assert task.configuration["query"]["query"] == "SELECT 1"

    def test_injects_reservation_into_insert_job_operator(self):
        """Test reservation injection into BigQueryInsertJobOperator."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="BigQueryInsertJobOperator",
            dag_id="my_dag",
            configuration={
                "query": {
                    "query": "SELECT * FROM table",
                    "useLegacySql": False,
                }
            },
        )

        with mock.patch.object(
            policy,
            "get_reservation",
            return_value="projects/p/locations/US/reservations/r",
        ):
            policy.task_policy(task)

        expected_sql = (
            "SET @@reservation='projects/p/locations/US/reservations/r';\n"
            "SELECT * FROM table"
        )
        assert task.configuration["query"]["query"] == expected_sql

    def test_skips_if_reservation_already_set(self):
        """Test that existing reservation statements are not duplicated."""
        from airflow_reservations import policy

        original_sql = "SET @@reservation='existing';\nSELECT 1"
        task = MockTask(
            task_id="my_task",
            task_type="BigQueryInsertJobOperator",
            dag_id="my_dag",
            configuration={"query": {"query": original_sql}},
        )

        with mock.patch.object(policy, "get_reservation", return_value="new_res"):
            policy.task_policy(task)

        # SQL should be unchanged
        assert task.configuration["query"]["query"] == original_sql

    def test_no_injection_without_matching_config(self):
        """Test that tasks without config entries are not modified."""
        from airflow_reservations import policy

        original_sql = "SELECT * FROM table"
        task = MockTask(
            task_id="my_task",
            task_type="BigQueryInsertJobOperator",
            dag_id="my_dag",
            configuration={"query": {"query": original_sql}},
        )

        with mock.patch.object(policy, "get_reservation", return_value=None):
            policy.task_policy(task)

        # SQL should be unchanged
        assert task.configuration["query"]["query"] == original_sql

    def test_injects_reservation_into_execute_query_operator(self):
        """Test reservation injection into BigQueryExecuteQueryOperator."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="BigQueryExecuteQueryOperator",
            dag_id="my_dag",
            sql="SELECT * FROM table",
        )

        with mock.patch.object(
            policy,
            "get_reservation",
            return_value="projects/p/locations/US/reservations/r",
        ):
            policy.task_policy(task)

        expected_sql = (
            "SET @@reservation='projects/p/locations/US/reservations/r';\n"
            "SELECT * FROM table"
        )
        assert task.sql == expected_sql

    def test_handles_sql_list_in_execute_query_operator(self):
        """Test reservation injection when sql is a list of statements."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="BigQueryExecuteQueryOperator",
            dag_id="my_dag",
            sql=["SELECT 1", "SELECT 2"],
        )

        with mock.patch.object(
            policy,
            "get_reservation",
            return_value="projects/p/locations/US/reservations/r",
        ):
            policy.task_policy(task)

        # First statement should have reservation prepended
        assert task.sql[0].startswith("SET @@reservation")
        assert "SELECT 1" in task.sql[0]
        # Second statement unchanged
        assert task.sql[1] == "SELECT 2"


class TestGetTaskIdentifiers:
    """Tests for _get_task_identifiers function."""

    def test_gets_dag_id_from_attribute(self):
        """Test extracting dag_id from task.dag_id attribute."""
        from airflow_reservations.policy import _get_task_identifiers

        task = MockTask(task_id="task_1", task_type="Test", dag_id="dag_a")

        dag_id, task_id = _get_task_identifiers(task)

        assert dag_id == "dag_a"
        assert task_id == "task_1"

    def test_fallback_to_unknown_dag(self):
        """Test fallback when dag_id is not available."""
        from airflow_reservations.policy import _get_task_identifiers

        task = MockTask(task_id="task_1", task_type="Test", dag_id=None)

        dag_id, task_id = _get_task_identifiers(task)

        assert dag_id == "unknown_dag"
        assert task_id == "task_1"
