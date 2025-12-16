from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup


class MockBigQueryInsertJobOperator(BaseOperator):
    """
    Mock BigQueryInsertJobOperator for testing.
    
    This operator mimics the interface of BigQueryInsertJobOperator
    but just logs the configuration instead of executing.
    """
    
    # This must match what the policy looks for
    task_type = "BigQueryInsertJobOperator"
    
    def __init__(self, configuration: dict, **kwargs):
        super().__init__(**kwargs)
        self.configuration = configuration
    
    def execute(self, context: Context):
        query = self.configuration.get("query", {}).get("query", "")
        self.log.info("=" * 60)
        self.log.info("MockBigQueryInsertJobOperator executing")
        self.log.info("=" * 60)
        self.log.info("SQL Query:")
        self.log.info(query)
        self.log.info("=" * 60)
        
        # Check if reservation was injected
        if "SET @@reservation_id" in query:
            # Check if it's the "none" on-demand value
            if "= 'none'" in query:
                self.log.info("✅ SUCCESS: On-demand reservation (none) was injected!")
            else:
                self.log.info("✅ SUCCESS: Reservation was injected!")
        else:
            self.log.warning("❌ FAILURE: No reservation found in SQL!")
        
        return {"sql": query}


class MockBigQueryExecuteQueryOperator(BaseOperator):
    """
    Mock BigQueryExecuteQueryOperator for testing.
    """
    
    task_type = "BigQueryExecuteQueryOperator"
    
    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
    
    def execute(self, context: Context):
        self.log.info("=" * 60)
        self.log.info("MockBigQueryExecuteQueryOperator executing")
        self.log.info("=" * 60)
        self.log.info("SQL Query:")
        self.log.info(self.sql)
        self.log.info("=" * 60)
        
        if "SET @@reservation_id" in self.sql:
            if "= 'none'" in self.sql:
                self.log.info("✅ SUCCESS: On-demand reservation (none) was injected!")
            else:
                self.log.info("✅ SUCCESS: Reservation was injected!")
        else:
            self.log.warning("❌ FAILURE: No reservation found in SQL!")
        
        return {"sql": self.sql}


default_args = {
    "owner": "masthead",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="test_reservation_dag",
    default_args=default_args,
    description="E2E test DAG for masthead-airflow-policy",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "masthead", "e2e"],
) as dag:
    
    # Task 1: BigQueryInsertJobOperator with reservation path
    bq_insert_job = MockBigQueryInsertJobOperator(
        task_id="bq_insert_job_task",
        configuration={
            "query": {
                "query": "SELECT * FROM `project.dataset.table` WHERE date = CURRENT_DATE()",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )
    
    # Task 2: BigQueryExecuteQueryOperator with reservation path
    bq_execute_query = MockBigQueryExecuteQueryOperator(
        task_id="bq_execute_query_task",
        sql="INSERT INTO `project.dataset.results` SELECT * FROM `project.dataset.source`",
    )
    
    # Task 3: On-demand task (reservation = "none")
    # Should get SET @@reservation_id = 'none'; injected
    bq_ondemand = MockBigQueryInsertJobOperator(
        task_id="bq_ondemand_task",
        configuration={
            "query": {
                "query": "SELECT 1 AS on_demand_query",
                "useLegacySql": False,
            }
        },
    )
    
    # Task 4: Task NOT in config (should NOT get any reservation)
    bq_no_reservation = MockBigQueryInsertJobOperator(
        task_id="bq_no_reservation_task",
        configuration={
            "query": {
                "query": "SELECT 1 AS test",
                "useLegacySql": False,
            }
        },
    )

    # Task 5: Task inside a TaskGroup
    with TaskGroup("my_group") as tg:
        bq_nested_task = MockBigQueryInsertJobOperator(
            task_id="nested_task",
            configuration={
                "query": {
                    "query": "SELECT * FROM `project.dataset.nested`",
                    "useLegacySql": False,
                }
            },
        )
    
    bq_insert_job >> bq_execute_query >> bq_ondemand >> bq_no_reservation >> tg
