"""
Unit tests for bronze_to_silver.normalizer module.
Tests data normalization logic including type casting, NaN handling, and string trimming.
"""
import pytest
import polars as pl
from datetime import datetime, timezone

# Import the function to test
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.normalizer import normalize_dataset


class TestNormalizeDataset:
    """Test suite for normalize_dataset function."""

    def test_normalize_string_trimming(self):
        """Test that strings are properly trimmed."""
        schema_contract = {
            "columns": {
                "name": pl.String(),
                "value": pl.Int32()
            }
        }
        
        lf = pl.LazyFrame({
            "name": ["  test  ", "hello   ", "  world"],
            "value": [1, 2, 3]
        })
        
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        assert df["name"].to_list() == ["test", "hello", "world"]

    def test_normalize_float_nan_handling(self):
        """Test that NaN and infinite values are converted to None."""
        import math
        schema_contract = {
            "columns": {
                "measurement": pl.Float64()
            }
        }
        
        lf = pl.LazyFrame({
            "measurement": [1.0, float('nan'), float('inf'), 5.0]
        })
        
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        values = df["measurement"].to_list()
        assert values[0] == 1.0
        assert values[1] is None  # NaN should be None
        assert values[2] is None  # Inf should be None
        assert values[3] == 5.0

    def test_normalize_datetime_casting(self):
        """Test that datetimes are cast to microseconds."""
        schema_contract = {
            "columns": {
                "timestamp": pl.Datetime()
            }
        }
        
        lf = pl.LazyFrame({
            "timestamp": [datetime(2024, 1, 1, tzinfo=timezone.utc)]
        })
        
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        # Check that timestamp is in microseconds
        assert df["timestamp"].dtype.time_unit == "us"

    def test_normalize_adds_silver_processed_at(self):
        """Test that _silver_processed_at column is added."""
        schema_contract = {
            "columns": {
                "id": pl.Int32()
            }
        }
        
        lf = pl.LazyFrame({"id": [1, 2, 3]})
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        assert "_silver_processed_at" in df.columns
        assert len(df["_silver_processed_at"]) == 3

    def test_normalize_type_casting(self):
        """Test that columns are cast to expected types."""
        schema_contract = {
            "columns": {
                "id": pl.Int32(),
                "value": pl.Float64()
            }
        }
        
        # Create data with different but compatible types
        lf = pl.LazyFrame({
            "id": [1, 2, 3],  # Will be Int64 by default
            "value": [1.5, 2.5, 3.5]
        })
        
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        assert df["id"].dtype == pl.Int32
        assert df["value"].dtype == pl.Float64

    def test_normalize_empty_dataframe(self):
        """Test normalization of empty dataframe."""
        schema_contract = {
            "columns": {
                "id": pl.Int32(),
                "name": pl.String()
            }
        }
        
        lf = pl.LazyFrame({"id": pl.Series([], dtype=pl.Int32), "name": pl.Series([], dtype=pl.String)})
        result = normalize_dataset(lf, "test_dataset", schema_contract)
        df = result.collect()
        
        assert df.height == 0
        assert "_silver_processed_at" in df.columns

    def test_normalize_schema_mismatch_raises_error(self):
        """Test that missing columns in source raise an error during select."""
        schema_contract = {
            "columns": {
                "id": pl.Int32(),
                "missing_col": pl.String()
            }
        }
        
        lf = pl.LazyFrame({"id": [1, 2, 3]})
        
        # This should raise an error because missing_col doesn't exist
        with pytest.raises(pl.exceptions.ColumnNotFoundError):
            result = normalize_dataset(lf, "test_dataset", schema_contract)
            result.collect()
