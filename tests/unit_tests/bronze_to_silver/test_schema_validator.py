"""
Unit tests for bronze_to_silver.schema_validator module.
Tests schema validation logic including column presence and type matching.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.schema_validator import validate_dataset_schema, AirflowFailException


class TestSchemaValidator:
    """Test suite for validate_dataset_schema function."""

    def test_validate_schema_valid(self):
        """Test that valid schema passes without error."""
        expected_schema = {
            "id": pl.Int64(),  # Changed to Int64 to match Polars default
            "name": pl.String(),
            "value": pl.Float64()
        }
        
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0]
        })
        
        # Should not raise any exception
        validate_dataset_schema(lf, "test_dataset", expected_schema)

    def test_validate_schema_missing_column(self):
        """Test that missing columns raise AirflowFailException."""
        expected_schema = {
            "id": pl.Int32(),
            "missing_col": pl.String()
        }
        
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"]
        })
        
        with pytest.raises(AirflowFailException) as exc_info:
            validate_dataset_schema(lf, "test_dataset", expected_schema)
        
        assert "Missing mandatory columns" in str(exc_info.value)
        assert "missing_col" in str(exc_info.value)

    def test_validate_schema_type_mismatch(self):
        """Test that type mismatches raise AirflowFailException."""
        expected_schema = {
            "id": pl.Int32(),
            "value": pl.String()  # Expected String but got Float64
        }
        
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [1.0, 2.0, 3.0]  # Float64
        })
        
        with pytest.raises(AirflowFailException) as exc_info:
            validate_dataset_schema(lf, "test_dataset", expected_schema)
        
        assert "Type mismatches" in str(exc_info.value)

    def test_validate_schema_extra_columns_allowed(self):
        """Test that extra columns in data are allowed (schema drift)."""
        expected_schema = {
            "id": pl.Int64()  # Changed to Int64 to match Polars default
        }
        
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "extra_col": ["a", "b", "c"],
            "another_extra": [1.0, 2.0, 3.0]
        })
        
        # Should not raise - extra columns are allowed
        validate_dataset_schema(lf, "test_dataset", expected_schema)

    def test_validate_schema_datetime_type(self):
        """Test datetime type validation with timezone."""
        from datetime import datetime, timezone
        
        expected_schema = {
            "timestamp": pl.Datetime()
        }
        
        lf = pl.LazyFrame({
            "timestamp": [datetime(2024, 1, 1, tzinfo=timezone.utc)]
        })
        
        # Should pass - Datetime matches Datetime base type
        validate_dataset_schema(lf, "test_dataset", expected_schema)

    def test_validate_schema_empty_dataframe(self):
        """Test validation of empty dataframe with correct schema."""
        expected_schema = {
            "id": pl.Int32(),
            "name": pl.String()
        }
        
        lf = pl.LazyFrame({
            "id": pl.Series([], dtype=pl.Int32),
            "name": pl.Series([], dtype=pl.String)
        })
        
        # Should not raise - empty but schema is correct
        validate_dataset_schema(lf, "test_dataset", expected_schema)

    def test_validate_schema_multiple_errors(self):
        """Test that both missing columns and type mismatches are reported."""
        expected_schema = {
            "id": pl.Int32(),
            "missing_col": pl.String(),
            "value": pl.Int32()  # Expected Int32 but got Float64
        }
        
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [1.0, 2.0, 3.0]
        })
        
        with pytest.raises(AirflowFailException) as exc_info:
            validate_dataset_schema(lf, "test_dataset", expected_schema)
        
        error_msg = str(exc_info.value)
        assert "Missing mandatory columns" in error_msg
        assert "missing_col" in error_msg
        assert "Type mismatches" in error_msg
        assert "value" in error_msg
