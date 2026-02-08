"""Airflow Cluster Policy for BigQuery reservation management.

This module implements the task_policy hook that automatically injects
BigQuery reservation assignments into BigQuery operators based on
the Masthead configuration. It prioritizes using the reservationId
field in the job configuration.
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
        "BigQueryGetDataOperator",
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
    """Inject reservation into BigQuery operator configuration.

    Modifies the task's configuration dict to set the reservationId.

    Args:
        task: The BigQuery operator task.
        reservation_id: The reservation ID to inject.

    Returns:
        True if injection was successful, False otherwise.
    """
    if not hasattr(task, "configuration"):
        return False

    configuration = task.configuration
    if not isinstance(configuration, dict):
        logger.debug("Task %s configuration is not a dict", task.task_id)
        return False

    # Set reservationId in the query configuration
    # This works for Standard and Legacy SQL and doesn't modify the query string.
    query_config = configuration.setdefault("query", {})
    if not isinstance(query_config, dict):
        logger.debug("Task %s query config is not a dict", task.task_id)
        return False

    # Check if reservation is already set (idempotency)
    existing_reservation = query_config.get("reservationId")
    if existing_reservation == reservation_id:
        logger.debug(
            "Task %s already has reservation %s set, skipping",
            task.task_id,
            reservation_id,
        )
        return False

    query_config["reservationId"] = reservation_id

    logger.debug(
        "Injected reservationId %s into task %s configuration",
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
    """Airflow cluster policy hook for BigQuery reservation injection."""
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

    # 1. First priority: Inject into configuration (safest, supports Legacy SQL)
    if _inject_reservation_into_configuration(task, reservation_id):
        return

    # 2. Second priority: Fallback to SQL injection for specific "Job" operators
    # ONLY if they use Standard SQL (SET is not supported in Legacy SQL).
    # Check operators and others that don't support multistatement scripts are skipped.
    if task.task_type in ("BigQueryInsertJobOperator", "BigQueryExecuteQueryOperator"):
        use_legacy_sql = getattr(task, "use_legacy_sql", None)

        # If not explicitly on the task, check configuration if available
        if use_legacy_sql is None and hasattr(task, "configuration"):
            config = getattr(task, "configuration", {})
            if isinstance(config, dict):
                use_legacy_sql = config.get("query", {}).get("useLegacySql")

        # BigQuery defaults to Standard SQL (False) if not specified
        if use_legacy_sql is True:
            logger.debug(
                "Task %s uses Legacy SQL and has no configuration attribute. "
                "Skipping SQL injection.",
                task.task_id,
            )
            return

        _inject_reservation_into_sql_attribute(task, reservation_id)
