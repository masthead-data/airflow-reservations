"""Airflow Cluster Policy for BigQuery reservation management.

This module implements the task_policy hook that automatically injects
BigQuery reservation assignments into BigQuery operators based on
the Masthead configuration.
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
    reservation: str,
) -> bool:
    """Inject reservation into BigQuery operator configuration.

    Modifies the task's configuration dict to set the reservation parameter.
    This is safer than SQL injection as it doesn't modify the query string.

    Args:
        task: The BigQuery operator task.
        reservation: The reservation ID to inject.

    Returns:
        True if injection was successful, False otherwise.
    """
    # If the task's configuration is None or missing, we try to initialize it.
    if getattr(task, "configuration", None) is None:
        try:
            setattr(task, "configuration", {})
            logger.info("Initialized configuration attribute for task %s", task.task_id)
        except Exception as e:
            logger.debug(
                "Could not add configuration attribute to task %s: %s", task.task_id, e
            )
            return False

    configuration = task.configuration
    if not isinstance(configuration, dict):
        logger.debug("Task %s configuration is not a dict", task.task_id)
        return False

    # 1. Set 'reservation' at the top level of JobConfiguration (Standard BigQuery API)
    # This is the most robust way to set the reservation for any job type.
    existing_reservation = configuration.get("reservation")
    if existing_reservation == reservation:
        logger.debug(
            "Task %s already has reservation %s set at top-level",
            task.task_id,
            reservation,
        )
    else:
        configuration["reservation"] = reservation

    logger.info(
        "Injected reservation %s into task %s configuration",
        reservation,
        task.task_id,
    )
    return True


def _inject_reservation_into_sql_attribute(
    task: BaseOperator,
    reservation: str,
) -> bool:
    """Inject reservation into BigQuery operator sql attribute via SET statement.

    Args:
        task: The BigQuery operator task.
        reservation: The reservation ID to inject.

    Returns:
        True if injection was successful, False otherwise.
    """
    # Check for legacy SQL usage (SET @@reservation not supported)
    use_legacy_sql = getattr(task, "use_legacy_sql", None)

    # Fallback to checking configuration if attribute missing
    if use_legacy_sql is None and hasattr(task, "configuration"):
        config = getattr(task, "configuration", {})
        if isinstance(config, dict):
            use_legacy_sql = config.get("query", {}).get("useLegacySql")

    if use_legacy_sql is True:
        logger.debug(
            "Task %s uses Legacy SQL and cannot use SQL injection for reservation. Skipping.",
            task.task_id,
        )
        return False

    if not hasattr(task, "sql"):
        return False

    original_sql = task.sql
    if not original_sql:
        return False

    # Handle both string and list of strings
    if isinstance(original_sql, str):
        if "SET @@reservation" in original_sql:
            return False

        reservation_sql = format_reservation_sql(reservation)
        task.sql = reservation_sql + original_sql

    elif isinstance(original_sql, (list, tuple)):
        # For multiple SQL statements, prepend to the first one
        if not original_sql:
            return False

        first_sql = original_sql[0]
        if "SET @@reservation" in str(first_sql):
            return False

        reservation_sql = format_reservation_sql(reservation)
        modified_list = [reservation_sql + str(first_sql)] + list(original_sql[1:])
        task.sql = modified_list
    else:
        return False

    logger.info(
        "Injected reservation %s into task %s via SQL injection",
        reservation,
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
    reservation = get_reservation(dag_id, task_id)
    if not reservation:
        return

    # 1. BigQueryInsertJobOperator: Always uses configuration dict
    if task.task_type == "BigQueryInsertJobOperator":
        _inject_reservation_into_configuration(task, reservation)
        return

    # 2. BigQueryExecuteQueryOperator: Uses SQL string input
    if task.task_type == "BigQueryExecuteQueryOperator":
        _inject_reservation_into_sql_attribute(task, reservation)
        return

    # 3. Unsupported operators: Skip
    logger.debug(
        "Skipping reservation injection for unsupported operator type: %s",
        task.task_type,
    )
