"""Airflow Cluster Policy for BigQuery reservation management.

This module implements the task_policy hook that automatically injects
BigQuery reservation assignments into BigQueryInsertJobOperator and
BigQueryExecuteQueryOperator tasks based on the Masthead configuration.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.policies import hookimpl

from airflow_reservations.config import (
    format_reservation_sql,
    get_reservation,
)

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)

# BigQuery operator types that we intercept
BIGQUERY_OPERATOR_TYPES = frozenset(
    {
        "BigQueryInsertJobOperator",
        "BigQueryExecuteQueryOperator",  # Deprecated in newer versions
        "BigQueryCheckOperator",
        "BigQueryValueCheckOperator",
        "BigQueryIntervalCheckOperator",
        "BigQueryColumnCheckOperator",
        "BigQueryTableCheckOperator",
    }
)


def _get_task_identifiers(task: BaseOperator) -> tuple[str, str]:
    """Extract dag_id and task_id from a task.

    Args:
        task: The Airflow task operator.

    Returns:
        Tuple of (dag_id, task_id).
    """
    dag_id = getattr(task, "dag_id", None)
    if dag_id is None and hasattr(task, "dag") and task.dag is not None:
        dag_id = task.dag.dag_id

    return dag_id or "unknown_dag", task.task_id


def _inject_reservation_into_configuration(
    task: BaseOperator,
    reservation_id: str,
) -> bool:
    """Inject reservation into BigQueryInsertJobOperator configuration.

    Modifies the task's configuration dict to prepend SET @@reservation
    to the SQL query.

    Args:
        task: The BigQuery operator task.
        reservation_id: The reservation ID to inject.

    Returns:
        True if injection was successful, False otherwise.
    """
    if not hasattr(task, "configuration"):
        logger.debug("Task %s has no configuration attribute", task.task_id)
        return False

    configuration = task.configuration
    if not isinstance(configuration, dict):
        logger.debug("Task %s configuration is not a dict", task.task_id)
        return False

    query_config = configuration.get("query", {})
    if not isinstance(query_config, dict):
        logger.debug("Task %s query config is not a dict", task.task_id)
        return False

    original_sql = query_config.get("query", "")
    if not original_sql:
        logger.debug("Task %s has no SQL query", task.task_id)
        return False

    # Check if reservation is already set (idempotency)
    if "SET @@reservation" in original_sql:
        logger.debug(
            "Task %s already has reservation set, skipping",
            task.task_id,
        )
        return False

    # Prepend the reservation SET statement
    reservation_sql = format_reservation_sql(reservation_id)
    new_sql = reservation_sql + original_sql

    # Mutate the configuration in place
    task.configuration["query"]["query"] = new_sql

    logger.info(
        "Injected reservation %s into task %s",
        reservation_id,
        task.task_id,
    )
    return True


def _inject_reservation_into_sql_attribute(
    task: BaseOperator,
    reservation_id: str,
) -> bool:
    """Inject reservation into BigQueryExecuteQueryOperator sql attribute.

    Args:
        task: The BigQuery operator task.
        reservation_id: The reservation ID to inject.

    Returns:
        True if injection was successful, False otherwise.
    """
    if not hasattr(task, "sql"):
        logger.debug("Task %s has no sql attribute", task.task_id)
        return False

    original_sql = task.sql
    if not original_sql:
        logger.debug("Task %s has no SQL", task.task_id)
        return False

    # Handle both string and list of strings
    if isinstance(original_sql, str):
        if "SET @@reservation" in original_sql:
            logger.debug(
                "Task %s already has reservation set, skipping",
                task.task_id,
            )
            return False

        reservation_sql = format_reservation_sql(reservation_id)
        task.sql = reservation_sql + original_sql

    elif isinstance(original_sql, (list, tuple)):
        # For multiple SQL statements, prepend to the first one
        if not original_sql:
            return False

        first_sql = original_sql[0]
        if "SET @@reservation" in str(first_sql):
            logger.debug(
                "Task %s already has reservation set, skipping",
                task.task_id,
            )
            return False

        reservation_sql = format_reservation_sql(reservation_id)
        modified_list = [reservation_sql + str(first_sql)] + list(original_sql[1:])
        task.sql = modified_list
    else:
        logger.debug("Task %s sql is not a string or list", task.task_id)
        return False

    logger.info(
        "Injected reservation %s into task %s",
        reservation_id,
        task.task_id,
    )
    return True


@hookimpl
def task_policy(task: BaseOperator) -> None:
    """Airflow cluster policy hook for BigQuery reservation injection.

    This function is called by Airflow for every task when it is loaded
    from the DagBag. For BigQuery tasks that have a matching entry in
    the Masthead configuration, it prepends the reservation assignment
    to the SQL query.

    Args:
        task: The task operator being processed.
    """
    # Only process BigQuery operators
    if task.task_type not in BIGQUERY_OPERATOR_TYPES:
        return

    # Get task identifiers
    dag_id, task_id = _get_task_identifiers(task)

    # Look up reservation for this task
    reservation_id = get_reservation(dag_id, task_id)
    if not reservation_id:
        logger.debug(
            "No reservation configured for %s.%s",
            dag_id,
            task_id,
        )
        return

    # Try injection methods based on operator type
    if task.task_type == "BigQueryInsertJobOperator":
        _inject_reservation_into_configuration(task, reservation_id)
    else:
        # All other BigQuery operators use sql attribute (ExecuteQueryOperator, Check operators, etc.)
        _inject_reservation_into_sql_attribute(task, reservation_id)
