"""Configuration loading for Airflow Reservations Policy.

This module provides utilities for loading reservation mappings from
the Masthead configuration file. Users can either rely on the automatic
policy injection for BigQuery operators, or use get_reservation() directly
in Python operators for custom BigQuery API calls.

Config format:
{
    "reservation_config": [
        {
            "tag": "editions",
            "reservation": "projects/{project}/locations/{location}/reservations/{name}",
            "tasks": ["dag_id.task_id", ...]
        },
        {
            "tag": "on_demand",
            "reservation": "none",
            "tasks": ["dag_id.task_id", ...]
        },
        {
            "tag": "skip",
            "reservation": null,
            "tasks": ["dag_id.task_id", ...]
        }
    ]
}

Reservation value semantics:
- Full path (e.g., "projects/.../reservations/...") → Injects that reservation
- "none" → Injects SET @@reservation='none'; (explicitly use on-demand capacity)
- null → Skips the task entirely (no SQL modification)

Example usage in a PythonOperator:
    from google.cloud import bigquery
    from airflow_reservations import get_reservation

    def my_bigquery_task(**context):
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        reservation = get_reservation(dag_id, task_id)

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()

        if reservation:
            job_config.reservation = reservation

        query_job = client.query(sql, job_config=job_config)
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

# Default config path - can be overridden via environment variable
DEFAULT_CONFIG_PATH = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
    "dags",
    "reservations_config.json",
)

# Environment variable to override config path
CONFIG_PATH_ENV_VAR = "RESERVATIONS_CONFIG_PATH"

# Sentinel value to indicate a task should be skipped (not in lookup)
_SKIP_TASK = object()

# Cache lock for thread-safe config reloading
_config_lock = threading.Lock()
_config_cache: dict[str, Any] | None = None
_config_mtime: float = 0.0
# Derived lookup table: task_key -> reservation (str) or _SKIP_TASK sentinel
_reservation_lookup: dict[str, str | object] = {}


def get_config_path() -> str:
    """Get the path to the Masthead configuration file.

    Returns:
        Path to config file, from RESERVATIONS_CONFIG_PATH env var or default location.
    """
    return os.environ.get(CONFIG_PATH_ENV_VAR, DEFAULT_CONFIG_PATH)


def _build_reservation_lookup(config: dict[str, Any]) -> dict[str, str | object]:
    """Build a lookup table from task key to reservation.

    Semantics:
    - reservation = "projects/..." → Store the path (will be injected)
    - reservation = "none" → Store "none" (will inject SET @@reservation='none')
    - reservation = null → Do NOT add to lookup (task will be skipped)

    Args:
        config: The loaded configuration dictionary.

    Returns:
        Dictionary mapping 'dag_id.task_id' to reservation ID string.
        Tasks with null reservation are NOT included (they get skipped).
    """
    lookup: dict[str, str | object] = {}

    reservation_config = config.get("reservation_config", [])

    for entry in reservation_config:
        if not isinstance(entry, dict):
            continue

        reservation = entry.get("reservation")
        tasks = entry.get("tasks", [])

        # null reservation means skip this task (don't add to lookup)
        if reservation is None:
            continue

        # "none" and other string values are stored as-is
        for task_key in tasks:
            if isinstance(task_key, str):
                lookup[task_key] = reservation

    return lookup


def load_config(force_reload: bool = False) -> dict[str, Any]:
    """Load the Masthead configuration file.

    The config is cached and only reloaded when the file modification time changes.
    This provides good performance while still picking up config updates.

    Args:
        force_reload: If True, ignore cache and reload from disk.

    Returns:
        Dictionary with configuration, or empty dict if file doesn't exist or is invalid.
    """
    global _config_cache, _config_mtime, _reservation_lookup

    config_path = get_config_path()

    try:
        # Check if file exists
        if not os.path.exists(config_path):
            logger.debug("Masthead config file not found: %s", config_path)
            return {}

        # Check file modification time for cache invalidation
        current_mtime = os.path.getmtime(config_path)

        with _config_lock:
            if (
                not force_reload
                and _config_cache is not None
                and current_mtime == _config_mtime
            ):
                return _config_cache

            # Load and parse config
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            _config_cache = config
            _config_mtime = current_mtime

            # Rebuild lookup table
            _reservation_lookup = _build_reservation_lookup(config)

            logger.info("Loaded Masthead config from %s", config_path)
            return config

    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in Masthead config %s: %s", config_path, e)
        return {}
    except OSError as e:
        logger.error("Error reading Masthead config %s: %s", config_path, e)
        return {}
    except Exception as e:
        # Catch-all to ensure Airflow doesn't crash on config errors
        logger.error("Unexpected error loading Masthead config: %s", e)
        return {}


def get_reservation(dag_id: str, task_id: str) -> str | None:
    """Get the reservation ID for a specific task.

    This function is the public API for users who want to apply reservations
    in their own Python operators when making direct BigQuery API calls.

    Semantics:
    - Returns the reservation string (including "none" for on-demand)
    - Returns None if the task is not in the config OR if reservation is null

    Args:
        dag_id: The DAG ID.
        task_id: The task ID (including TaskGroup prefix if applicable).

    Returns:
        Reservation ID string if found (including "none"), None otherwise.

    Example:
        >>> from google.cloud import bigquery
        >>> from airflow_reservations import get_reservation
        >>> reservation = get_reservation("my_dag", "my_task")
        >>> job_config = bigquery.QueryJobConfig()
        >>> if reservation:
        ...     job_config.reservation = reservation
    """
    # Ensure config is loaded
    load_config()

    lookup_key = f"{dag_id}.{task_id}"

    # Return the reservation if in lookup, None otherwise
    result = _reservation_lookup.get(lookup_key)
    if isinstance(result, str):
        return result
    return None


def get_reservation_entry(dag_id: str, task_id: str) -> dict[str, Any] | None:
    """Get the full reservation config entry for a specific task.

    This provides access to the tag and other metadata for the task.

    Args:
        dag_id: The DAG ID.
        task_id: The task ID.

    Returns:
        The config entry dict if found, None otherwise.
    """
    config = load_config()
    lookup_key = f"{dag_id}.{task_id}"

    for entry in config.get("reservation_config", []):
        if isinstance(entry, dict):
            tasks = entry.get("tasks", [])
            if lookup_key in tasks:
                return entry

    return None


def format_reservation_sql(reservation_id: str) -> str:
    """Format the SQL statement to set a reservation.

    Args:
        reservation_id: The full reservation path or "none" for on-demand.

    Returns:
        SQL SET statement for the reservation.
    """
    return f"SET @@reservation='{reservation_id}';\n"
