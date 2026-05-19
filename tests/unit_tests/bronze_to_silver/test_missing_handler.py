"""
Unit tests for bronze_to_silver.missing_handler module.
Tests missing value handling strategies including fill_value and forward_fill.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.missing_handler import handle_missing_values


class TestMissingHandler:
    """Test suite for handle_missing_values function."""

    def test_handle_missing_no_rules(self):
        """Test that data without rules returns unchanged."""
        lf = pl.LazyFrame({
            "id": [1, 2, None],
            "value": [10.0, None, 30.0]
        })
        
        result = handle_missing_values(lf, "test_dataset", {})
        df = result.collect()
        
        assert df.height == 3
        assert df["value"].null_count() == 1  # Still has NULL

    def test_handle_missing_fill_value(self):
        """Test fill_value strategy replaces NULLs with specified value."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [10.0, None, 30.0]
        })
        
        rules = {
            "value": {"strategy": "fill_value", "value": 0.0}
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        assert df["value"].null_count() == 0
        assert df["value"].to_list() == [10.0, 0.0, 30.0]

    def test_handle_missing_fill_value_preserves_type(self):
        """Test that fill_value preserves column type."""
        lf = pl.LazyFrame({
            "id": pl.Series([1, 2, 3], dtype=pl.Int32),
            "value": pl.Series([10, None, 30], dtype=pl.Int32)  # Int32
        })
        
        rules = {
            "value": {"strategy": "fill_value", "value": 0}
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        assert df["value"].dtype == pl.Int32
        assert df["value"].to_list() == [10, 0, 30]

    def test_handle_missing_forward_fill(self):
        """Test forward_fill strategy propagates last known value."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3, 4, 5],
            "sensor_id": ["A", "A", "A", "B", "B"],
            "value": [10.0, None, 30.0, None, 50.0],
            "timestamp": [1, 2, 3, 4, 5]
        })
        
        rules = {
            "value": {
                "strategy": "forward_fill",
                "partition_by": "sensor_id",
                "order_by": "timestamp"
            }
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        # For sensor A: 10, 10 (filled), 30
        # For sensor B: None (no prior value), 50
        values = df.sort(["sensor_id", "timestamp"])["value"].to_list()
        assert values[0] == 10.0  # A, ts=1
        assert values[1] == 10.0  # A, ts=2 (filled from previous)
        assert values[2] == 30.0  # A, ts=3
        
        # B starts with None since no prior value in partition
        # But ts=5 should be 50

    def test_handle_missing_multiple_columns(self):
        """Test handling multiple columns with different strategies."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "pressure": [100.0, None, 300.0],
            "temperature": [None, 20.0, 25.0]
        })
        
        rules = {
            "pressure": {"strategy": "fill_value", "value": 0.0},
            "temperature": {"strategy": "fill_value", "value": -999.0}
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        assert df["pressure"].null_count() == 0
        assert df["temperature"].null_count() == 0
        assert df["pressure"][1] == 0.0
        assert df["temperature"][0] == -999.0

    def test_handle_missing_string_columns(self):
        """Test fill_value with string columns."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "status": ["active", None, "inactive"]
        })
        
        rules = {
            "status": {"strategy": "fill_value", "value": "unknown"}
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        assert df["status"].null_count() == 0
        assert df["status"].to_list() == ["active", "unknown", "inactive"]

    def test_handle_missing_partial_rules(self):
        """Test that only specified columns are processed."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value1": [10.0, None, 30.0],
            "value2": [None, 20.0, None]
        })
        
        rules = {
            "value1": {"strategy": "fill_value", "value": 0.0}
            # value2 not in rules, should remain with NULLs
        }
        
        result = handle_missing_values(lf, "test_dataset", rules)
        df = result.collect()
        
        assert df["value1"].null_count() == 0
        assert df["value2"].null_count() == 2  # Unchanged
