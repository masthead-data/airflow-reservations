import time
from airflow_reservations.config import get_reservation_entry, load_config, _config_cache

def setup():
    global _config_cache
    config = {
        "reservation_config": [
            {
                "tag": f"tag_{i}",
                "reservation": f"res_{i}" if i % 2 == 0 else None,
                "tasks": [f"dag.task_{i}_{j}" for j in range(100)]
            }
            for i in range(100)
        ]
    }
    import airflow_reservations.config as config_mod
    config_mod._config_cache = config
    config_mod._config_mtime = 1.0
    config_mod._reservation_lookup = config_mod._build_reservation_lookup(config)
    # also we should mock load_config
    config_mod.load_config = lambda *args, **kwargs: config
    return config

def run_benchmark():
    setup()

    start_time = time.time()
    for i in range(100):
        for j in range(100):
            get_reservation_entry("dag", f"task_{i}_{j}")
    end_time = time.time()
    print(f"Time taken: {(end_time - start_time) * 1000:.2f} ms")

if __name__ == "__main__":
    run_benchmark()
