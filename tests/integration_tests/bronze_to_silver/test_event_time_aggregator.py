"""Unit tests for event_time_aggregator.py module."""

import polars as pl
import pytest
from datetime import date, datetime
from plugins.bronze_to_silver.event_time_aggregator import aggregate_event_time_metrics


class TestAggregateEventTimeMetrics:
    """Tests for the aggregate_event_time_metrics function."""

    def test_aggregate_daily_mean_max(self):
        """Test daily aggregation with mean and max functions."""
        data = {
            "well_id": [1, 1, 1, 2, 2],
            "timestamp": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 14, 0),
                datetime(2024, 1, 2, 8, 0),
                datetime(2024, 1, 1, 9, 0),
                datetime(2024, 1, 1, 15, 0),
            ],
            "value": [10.0, 20.0, 30.0, 40.0, 60.0],
        }

        lf = pl.LazyFrame(data)

        rules = {
            "key": "well_id",
            "time_column": "timestamp",
            "granularity": "1d",
            "metrics": {"value": ["mean", "max"]},
        }

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", rules)
        result = result_lf.collect().sort(["well_id", "timestamp"])

        # Should have 3 rows: well 1 on 2 days, well 2 on 1 day
        assert result.shape[0] == 3

        # Check well 1, day 1: mean=(10+20)/2=15, max=20
        well1_day1 = result.filter(
            (pl.col("well_id") == 1) & (pl.col("timestamp") == date(2024, 1, 1))
        )
        assert well1_day1.shape[0] == 1
        assert abs(well1_day1["avg_value"][0] - 15.0) < 0.001
        assert well1_day1["max_value"][0] == 20.0

        # Check well 1, day 2: mean=30, max=30
        well1_day2 = result.filter(
            (pl.col("well_id") == 1) & (pl.col("timestamp") == date(2024, 1, 2))
        )
        assert well1_day2["avg_value"][0] == 30.0
        assert well1_day2["max_value"][0] == 30.0

    def test_aggregate_multiple_metrics(self):
        """Test aggregation with multiple metric columns."""
        data = {
            "well_id": [1, 1],
            "timestamp": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 14, 0),
            ],
            "temperature": [20.0, 30.0],
            "pressure": [100.0, 150.0],
        }

        lf = pl.LazyFrame(data)

        rules = {
            "key": "well_id",
            "time_column": "timestamp",
            "granularity": "1d",
            "metrics": {
                "temperature": ["mean", "max"],
                "pressure": ["sum", "min"],
            },
        }

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", rules)
        result = result_lf.collect()

        assert result.shape[0] == 1
        assert "avg_temperature" in result.columns
        assert "max_temperature" in result.columns
        assert "sum_pressure" in result.columns
        assert "min_pressure" in result.columns

        assert abs(result["avg_temperature"][0] - 25.0) < 0.001
        assert result["max_temperature"][0] == 30.0
        assert result["sum_pressure"][0] == 250.0
        assert result["min_pressure"][0] == 100.0

    def test_aggregate_empty_rules(self):
        """Test that empty rules return original dataframe."""
        data = {"id": [1, 2], "value": [10, 20]}
        lf = pl.LazyFrame(data)

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", {})
        result = result_lf.collect()

        assert result.shape[0] == 2
        assert result["id"].to_list() == [1, 2]

    def test_aggregate_with_date_type(self):
        """Test aggregation when time column is already Date type."""
        data = {
            "well_id": [1, 1],
            "date": [date(2024, 1, 1), date(2024, 1, 1)],
            "value": [10.0, 20.0],
        }

        lf = pl.LazyFrame(data)

        rules = {
            "key": "well_id",
            "time_column": "date",
            "granularity": "1d",
            "metrics": {"value": ["mean"]},
        }

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", rules)
        result = result_lf.collect()

        assert result.shape[0] == 1
        assert abs(result["avg_value"][0] - 15.0) < 0.001

    def test_aggregate_sum_function(self):
        """Test sum aggregation function."""
        data = {
            "well_id": [1, 1, 1],
            "timestamp": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 11, 0),
                datetime(2024, 1, 1, 12, 0),
            ],
            "production": [100.0, 200.0, 300.0],
        }

        lf = pl.LazyFrame(data)

        rules = {
            "key": "well_id",
            "time_column": "timestamp",
            "granularity": "1d",
            "metrics": {"production": ["sum"]},
        }

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", rules)
        result = result_lf.collect()

        assert result.shape[0] == 1
        assert result["sum_production"][0] == 600.0

    def test_aggregate_min_function(self):
        """Test min aggregation function."""
        data = {
            "well_id": [1, 1, 1],
            "timestamp": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 11, 0),
                datetime(2024, 1, 1, 12, 0),
            ],
            "temperature": [25.0, 20.0, 30.0],
        }

        lf = pl.LazyFrame(data)

        rules = {
            "key": "well_id",
            "time_column": "timestamp",
            "granularity": "1d",
            "metrics": {"temperature": ["min"]},
        }

        result_lf = aggregate_event_time_metrics(lf, "test_dataset", rules)
        result = result_lf.collect()

        assert result.shape[0] == 1
        assert result["min_temperature"][0] == 20.0
