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
    get_reservation,
)

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)

# BigQuery operator types that we intercept
BIGQUERY_OPERATOR_TYPES = frozenset(
    {
        "BigQueryInsertJobOperator",
        "BigQueryExecuteQueryOperator",
    }
)


def _init_debug_info():
    """One-time informational check for environment compatibility."""

    from importlib.metadata import PackageNotFoundError, version

    # Check for operators
    ops_found = []
    try:
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryInsertJobOperator,
        )

        ops_found.append("BigQueryInsertJobOperator")
    except ImportError:
        pass

    try:
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryExecuteQueryOperator,
        )

        ops_found.append("BigQueryExecuteQueryOperator")
    except ImportError:
        pass

    # Check version and reservation field support in bigquery library
    try:
        bq_version = version("google-cloud-bigquery")
    except PackageNotFoundError:
        bq_version = "unknown"

    try:
        provider_version = version("apache-airflow-providers-google")
    except PackageNotFoundError:
        provider_version = "unknown"

    try:
        airflow_version = version("apache-airflow")
    except PackageNotFoundError:
        airflow_version = "unknown"

    logger.debug(
        "Airflow Reservations Initialized: [Airflow %s] [Google Provider %s] [BigQuery Client %s] [Detected Operators: %s]",
        airflow_version,
        provider_version,
        bq_version,
        ", ".join(ops_found) if ops_found else "None",
    )


# Executed when module is loaded
_init_debug_info()


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


def _inject_reservation_into_task_attribute(
    task: BaseOperator,
    reservation: str,
    attribute_name: str,
) -> bool:
    """Inject reservation into a BigQuery operator's attribute.

    Args:
        task: The BigQuery operator task.
        reservation: The reservation ID to inject.
        attribute_name: The name of the task attribute to inject into
            (e.g., 'configuration' or 'api_resource_configs').

    Returns:
        True if injection was successful, False otherwise.
    """
    # Initialize attribute if it doesn't exist
    if getattr(task, attribute_name, None) is None:
        try:
            setattr(task, attribute_name, {})
            logger.info(
                "Initialized %s attribute for task %s", attribute_name, task.task_id
            )
        except Exception as e:
            logger.debug(
                "Could not add %s attribute to task %s: %s",
                attribute_name,
                task.task_id,
                e,
            )
            return False

    attribute_value = getattr(task, attribute_name)
    if not isinstance(attribute_value, dict):
        logger.debug("Task %s %s is not a dict", task.task_id, attribute_name)
        return False

    # Set reservation at the top level
    existing_reservation = attribute_value.get("reservation")
    if existing_reservation == reservation:
        logger.debug(
            "Task %s already has reservation %s set in %s",
            task.task_id,
            reservation,
            attribute_name,
        )
    else:
        attribute_value["reservation"] = reservation

    logger.info(
        "Injected reservation %s into task %s",
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

    # BigQueryInsertJobOperator: Uses configuration dict
    if task.task_type == "BigQueryInsertJobOperator":
        _inject_reservation_into_task_attribute(task, reservation, "configuration")
        return

    # BigQueryExecuteQueryOperator: Uses api_resource_configs
    if task.task_type == "BigQueryExecuteQueryOperator":
        _inject_reservation_into_task_attribute(
            task, reservation, "api_resource_configs"
        )
        return

    # 3. Unsupported operators: Skip
    logger.debug(
        "Skipping reservation injection for unsupported operator type: %s",
        task.task_type,
    )
