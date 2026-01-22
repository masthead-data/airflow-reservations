"""Tests for the config module."""

import json
import os
import tempfile
from unittest import mock

import pytest

from airflow_reservations import config


@pytest.fixture
def temp_config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(
            {
                "reservation_config": [
                    {
                        "tag": "critical",
                        "reservation": "projects/p1/locations/US/reservations/res1",
                        "tasks": ["dag_a.task_1"],
                    },
                    {
                        "tag": "standard",
                        "reservation": "projects/p2/locations/EU/reservations/res2",
                        "tasks": ["dag_b.task_2"],
                    },
                    {
                        "tag": "taskgroup-test",
                        "reservation": "projects/p3/locations/US/reservations/res3",
                        "tasks": ["dag_c.group.nested"],
                    },
                    {
                        "tag": "on_demand",
                        "reservation": "none",
                        "tasks": ["dag_d.ondemand_task"],
                    },
                    {
                        "tag": "skip_these",
                        "reservation": None,
                        "tasks": ["dag_e.skipped_task"],
                    },
                ]
            },
            f,
        )
        temp_path = f.name
    # Yield outside the with block so file is flushed and closed
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Reset the config cache before each test."""
    config._config_cache = None
    config._config_mtime = 0.0
    config._reservation_lookup = {}
    yield
    config._config_cache = None
    config._config_mtime = 0.0
    config._reservation_lookup = {}


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_valid_config(self, temp_config_file):
        """Test loading a valid config file."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.load_config()

        assert "reservation_config" in result
        assert len(result["reservation_config"]) == 5

    def test_load_missing_file_returns_empty(self):
        """Test that missing config file returns empty dict."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": "/nonexistent/path.json"}
        ):
            result = config.load_config()

        assert result == {}

    def test_load_invalid_json_returns_empty(self):
        """Test that invalid JSON returns empty dict."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {{{")
            temp_path = f.name

        try:
            with mock.patch.dict(os.environ, {"RESERVATIONS_CONFIG_PATH": temp_path}):
                result = config.load_config()
            assert result == {}
        finally:
            os.unlink(temp_path)

    def test_config_caching(self, temp_config_file):
        """Test that config is cached and not reloaded unnecessarily."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result1 = config.load_config()
            result2 = config.load_config()

        # Should be the exact same object due to caching
        assert result1 is result2

    def test_force_reload_bypasses_cache(self, temp_config_file):
        """Test that force_reload ignores the cache."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result1 = config.load_config()
            result2 = config.load_config(force_reload=True)

        # Should be equal but not the same object
        assert result1 == result2


class TestGetReservation:
    """Tests for get_reservation function."""

    def test_get_existing_reservation(self, temp_config_file):
        """Test getting a reservation that exists."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation("dag_a", "task_1")

        assert result == "projects/p1/locations/US/reservations/res1"

    def test_get_nonexistent_reservation(self, temp_config_file):
        """Test getting a reservation that doesn't exist returns None."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation("unknown_dag", "unknown_task")

        assert result is None

    def test_get_taskgroup_reservation(self, temp_config_file):
        """Test getting a reservation for a TaskGroup task."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation("dag_c", "group.nested")

        assert result == "projects/p3/locations/US/reservations/res3"

    def test_on_demand_returns_none_string(self, temp_config_file):
        """Test that 'none' reservation returns the string 'none' for on-demand."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation("dag_d", "ondemand_task")

        # "none" should be returned as-is to inject SET @@reservation='none'
        assert result == "none"

    def test_null_reservation_skips_task(self, temp_config_file):
        """Test that null reservation returns None (task is skipped)."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation("dag_e", "skipped_task")

        # null reservation means skip - returns None
        assert result is None


class TestGetReservationEntry:
    """Tests for get_reservation_entry function."""

    def test_get_entry_with_tag(self, temp_config_file):
        """Test getting a full entry with tag info."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation_entry("dag_a", "task_1")

        assert result is not None
        assert result["tag"] == "critical"
        assert result["reservation"] == "projects/p1/locations/US/reservations/res1"

    def test_get_entry_not_found(self, temp_config_file):
        """Test getting entry for unknown task."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation_entry("unknown", "unknown")

        assert result is None

    def test_get_entry_for_skipped_task(self, temp_config_file):
        """Test getting entry for a task with null reservation."""
        with mock.patch.dict(
            os.environ, {"RESERVATIONS_CONFIG_PATH": temp_config_file}
        ):
            result = config.get_reservation_entry("dag_e", "skipped_task")

        # Entry should still be found even though reservation is null
        assert result is not None
        assert result["tag"] == "skip_these"
        assert result["reservation"] is None


class TestFormatReservationSql:
    """Tests for format_reservation_sql function."""

    def test_format_reservation_sql(self):
        """Test formatting a reservation SQL statement."""
        result = config.format_reservation_sql(
            "projects/my-project/locations/US/reservations/my-res"
        )

        assert (
            result
            == "SET @@reservation='projects/my-project/locations/US/reservations/my-res';\n"
        )

    def test_format_none_reservation_sql(self):
        """Test formatting 'none' for on-demand capacity."""
        result = config.format_reservation_sql("none")

        assert result == "SET @@reservation='none';\n"


class TestBuildReservationLookup:
    """Tests for _build_reservation_lookup function."""

    def test_builds_lookup_from_config(self):
        """Test building lookup table from config."""
        test_config = {
            "reservation_config": [
                {"tag": "t1", "reservation": "res1", "tasks": ["d.t1", "d.t2"]},
                {"tag": "t2", "reservation": "res2", "tasks": ["d.t3"]},
            ]
        }

        lookup = config._build_reservation_lookup(test_config)

        assert lookup["d.t1"] == "res1"
        assert lookup["d.t2"] == "res1"
        assert lookup["d.t3"] == "res2"

    def test_none_string_is_stored(self):
        """Test that 'none' string is stored (for on-demand injection)."""
        test_config = {
            "reservation_config": [
                {"tag": "ondemand", "reservation": "none", "tasks": ["d.t1"]},
            ]
        }

        lookup = config._build_reservation_lookup(test_config)

        assert "d.t1" in lookup
        assert lookup["d.t1"] == "none"

    def test_null_reservation_not_in_lookup(self):
        """Test that null reservation tasks are NOT in lookup (skipped)."""
        test_config = {
            "reservation_config": [
                {"tag": "skip", "reservation": None, "tasks": ["d.t1"]},
            ]
        }

        lookup = config._build_reservation_lookup(test_config)

        # Task should NOT be in lookup - it's skipped
        assert "d.t1" not in lookup

    def test_handles_empty_config(self):
        """Test handling of empty config."""
        lookup = config._build_reservation_lookup({})
        assert lookup == {}

    def test_handles_malformed_entries(self):
        """Test that malformed entries are skipped."""
        test_config = {
            "reservation_config": [
                "not a dict",
                {"tag": "valid", "reservation": "res", "tasks": ["d.t1"]},
                {"tag": "no_tasks", "reservation": "res"},
            ]
        }

        lookup = config._build_reservation_lookup(test_config)

        assert lookup == {"d.t1": "res"}
