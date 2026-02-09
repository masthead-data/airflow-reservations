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
        api_resource_configs: dict = None,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.dag_id = dag_id
        self.configuration = configuration
        self.api_resource_configs = api_resource_configs
        self.dag = None


class TestTaskPolicy:
    """Tests for the task_policy function."""

    def test_ignores_non_bigquery_operators(self):
        """Test that non-BigQuery operators are ignored."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="CustomOperator",
            configuration={},
            api_resource_configs={},
        )

        with mock.patch.object(policy, "get_reservation", return_value="res1"):
            policy.task_policy(task)

        # Should not inject reservation into non-BigQuery operators
        assert "reservation" not in task.configuration
        assert "reservation" not in task.api_resource_configs

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
                }
            },
        )

        reservation = "projects/p/locations/US/reservations/r"
        with mock.patch.object(
            policy,
            "get_reservation",
            return_value=reservation,
        ):
            policy.task_policy(task)

        assert task.configuration["reservation"] == reservation

    def test_rewrites_existing_reservation(self):
        """Test that existing reservation statements are not duplicated."""
        from airflow_reservations import policy

        reservation = "projects/p/locations/US/reservations/r"
        task = MockTask(
            task_id="my_task",
            task_type="BigQueryInsertJobOperator",
            dag_id="my_dag",
            configuration={
                "query": {"query": "SELECT 1"},
                "reservation": "projects/p/locations/US/reservations/m",
            },
        )

        with mock.patch.object(policy, "get_reservation", return_value=reservation):
            policy.task_policy(task)

        assert task.configuration["reservation"] == reservation

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

        assert "reservation" not in task.configuration

    def test_injects_reservation_into_execute_query_operator(self):
        """Test reservation injection into BigQueryExecuteQueryOperator."""
        from airflow_reservations import policy

        task = MockTask(
            task_id="my_task",
            task_type="BigQueryExecuteQueryOperator",
            dag_id="my_dag",
            api_resource_configs={},
        )

        reservation = "projects/p/locations/US/reservations/r"
        with mock.patch.object(
            policy,
            "get_reservation",
            return_value=reservation,
        ):
            policy.task_policy(task)

        # Should use api_resource_configs for ExecuteQueryOperator
        assert task.api_resource_configs["reservation"] == reservation


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
