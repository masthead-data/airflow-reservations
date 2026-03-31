# API Reference

## `get_reservation(dag_id: str, task_id: str) -> str | None`

Look up the reservation ID for a specific task.

```python
from airflow_reservations import get_reservation

reservation = get_reservation("my_dag", "my_task")
# Returns: "projects/my-project/locations/US/reservations/my-res" or None
```

## `load_config(force_reload: bool = False) -> dict`

Load the full configuration dictionary.

```python
from airflow_reservations import load_config

config = load_config()
# Returns: {"reservations": {...}}
```
