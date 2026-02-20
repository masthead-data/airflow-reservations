# Airflow Reservations Policy

[![PyPI](https://img.shields.io/pypi/v/airflow-reservations.svg)](https://pypi.org/project/airflow-reservations/)

Airflow Cluster Policy plugin for BigQuery reservation management.

This package integrates with Airflow's [Cluster Policies](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html) to automatically inject BigQuery reservation assignments into your tasks without requiring any changes to your DAG code.

## Features

- **Automatic re-assignment** - intercepts BigQuery operators and re-assigns them to appropriate reservation based on the configuration:
  - `BigQueryInsertJobOperator` - Injects into `configuration.reservation` field
  - `BigQueryExecuteQueryOperator` - Injects into `api_resource_configs.reservation` field (supported in provider package 2.0.0 - 10.26.0)
- **Lookup-based Configuration** - Uses `dag_id.task_id` → `reservation` mappings
- **Python API** - Provides `get_reservation()` for custom BigQuery API calls in `PythonOperator`
- **Performance Optimized** - Config caching with file mtime-based invalidation
- **Graceful Error Handling** - Won't crash Airflow on config errors

## Installation

Add to your `requirements.txt`:

```text
airflow-reservations=X.Y.Z
```

## Configuration

### Generating Configuration

Use Masthead recommendations to generate the `reservations_config.json` file containing the optimal reservation assignments for your tasks. Users are responsible for pulling this configuration into their Airflow environment.

Typical workflow:

1. Masthead analyzes your BigQuery workloads
2. Read Masthead recommendations and generate `reservations_config.json` with optimal assignments
3. Merge the config into your DAGs repository
4. Airflow syncs the updated config file
5. The policy applies reservations on next task parse

### Configuration Fields

| Field         | Description                                    |
| ------------- | ---------------------------------------------- |
| `tag`         | Human-readable label for the reservation group |
| `reservation` | See values below                               |
| `tasks`       | Array of `"dag_id.task_id"` patterns           |

**Reservation values:**

| Value                                           | Behavior                              |
| ----------------------------------------------- | ------------------------------------- |
| `"projects/.../locations/.../reservations/..."` | Injects that reservation into the SQL |
| `"none"`                                        | Explicitly use on-demand capacity     |
| `null`                                          | Skips the task                        |

### Applying Configuration

Create a `reservations_config.json` file in your DAGs folder:

```json
{
  "reservation_config": [
    {
      "tag": "standard",
      "reservation": "projects/{project}/locations/{location}/reservations/{name}",
      "tasks": [
        "finance_dag.daily_report",
        "etl_dag.load_analytics",
        "etl_dag.etl_group.transform_data"
      ]
    },
    {
      "tag": "on_demand",
      "reservation": "none",
      "tasks": [
        "adhoc_dag.quick_query"
      ]
    },
    {
      "tag": "default",
      "reservation": null,
      "tasks": [
        "marketing_dag.calculate_roas"
      ]
    }
  ]
}
```

By default, the config file is loaded from `$AIRFLOW_HOME/dags/reservations_config.json`.

Override the path using the `RESERVATIONS_CONFIG_PATH` environment variable:

```bash
export RESERVATIONS_CONFIG_PATH=/custom/path/to/config.json
```

## How It Works

The policy is automatically registered via Airflow's plugin entrypoint system. When Airflow parses your DAGs, this plugin's `task_policy` hook is called for each task. For BigQuery tasks, it:

1. Extracts `dag_id` and `task_id` from the task
2. Looks up `dag_id.task_id` in the configuration file
3. If found, assigns the task to a reservation

## Prerequisite: BigQuery Job Labels

Masthead recommendations depend on BigQuery job labels that identify the Airflow task. Ensure every BigQuery job has:

- `airflow-dag`
- `airflow-task`

Airflow BigQuery operators set these labels by default (Airflow v2.9+), but custom job configuration can override defaults and drop them. When customizing operator configs, merge labels and keep both keys.

For jobs submitted in `PythonOperator` (or any manual BigQuery client usage), set labels explicitly (see `Usage in Python Operators` below).

Note: This package applies reservation assignment based on Airflow task identity, but it does not enforce BigQuery labels. Keep label handling in your DAG/operator/client code.

## Usage in Python Operators

For custom BigQuery API calls in `PythonOperator`, use the provided `get_reservation` function, e.g.:

```python
from airflow_reservations import get_reservation

def my_bigquery_task(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    existing_labels = {"team": "finance"}
    labels = {
      **existing_labels,
      "airflow-dag": dag_id,
      "airflow-task": task_id,
    }

    # Look up reservation for this task
    reservation = get_reservation(dag_id, task_id)

    # When executing, ensure labels and the reservation are included in the job config
    job_config = bigquery.QueryJobConfig(
      labels=labels,
      reservation=reservation if reservation else None
    )

    # Execute with BigQuery client...
    client.query(sql, job_config=job_config)
```

## Troubleshooting

### Config not loading

Check that:

1. The config file exists at the expected path
2. The file contains valid JSON
3. Airflow has read permissions

Enable debug logging:

```python
import logging
logging.getLogger("airflow_reservations").setLevel(logging.DEBUG)
```

### Reservations not being applied

Verify:

1. The task type is `BigQueryInsertJobOperator` or `BigQueryExecuteQueryOperator`
2. BigQuery jobs include `airflow-dag` and `airflow-task` labels (see `Prerequisite: BigQuery Job Labels` above)
3. The `dag_id.task_id` key exactly matches the config
4. For TaskGroups, the full path is included (e.g., `dag.group.task`)

## Supported Versions

This package is tested and compatible with:

- **Airflow 2.6+** (including 2.10.x) - ✅ Fully supported and tested
- **Airflow 3.x** (including 3.1.x) - ✅ Fully supported and tested (see Airflow 3 notes below)
- **Python 3.8+** (tested with 3.11 and 3.12)

### Airflow 3 Notes

In Airflow 3, the Task SDK parses DAGs in a separate process/container from the Scheduler and Worker. To ensure the policy works correctly:

1. **Installation**: The `airflow-reservations` package must be installed in the environment where the Task SDK executes (typically your custom Airflow image).
2. **Config Accessibility**: The `reservations_config.json` file must be accessible to the Task SDK process. If you are using remote DAG storage, ensure the config file is bundled with your DAGs or placed in a shared volume.
3. **Environment Variables**: If you use `RESERVATIONS_CONFIG_PATH`, it must be set in the environment of the worker/execution container as well.
