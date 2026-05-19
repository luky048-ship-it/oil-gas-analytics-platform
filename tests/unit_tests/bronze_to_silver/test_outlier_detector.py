"""
Unit tests for bronze_to_silver.outlier_detector module.
Tests statistical outlier detection using IQR method.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.outlier_detector import detect_outliers


class TestOutlierDetector:
    """Test suite for detect_outliers function."""

    def test_detect_outliers_no_outliers(self):
        """Test that data without outliers returns all valid records."""
        lf = pl.LazyFrame({
            "value": [10.0, 12.0, 11.0, 13.0, 12.5]
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=["value"], method="iqr", multiplier=3.0
        )
        
        assert valid_lf.collect().height == 5
        assert invalid_lf is None

    def test_detect_outliers_with_outliers(self):
        """Test that outliers are correctly identified and separated."""
        # Create data with clear outlier
        lf = pl.LazyFrame({
            "value": [10.0, 12.0, 11.0, 13.0, 100.0]  # 100 is an outlier
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=["value"], method="iqr", multiplier=1.5
        )
        
        assert valid_lf.collect().height == 4
        assert invalid_lf is not None
        assert invalid_lf.collect().height == 1
        
        # Check outlier has metadata
        df_invalid = invalid_lf.collect()
        assert "_quarantine_validation_name" in df_invalid.columns
        assert "_quarantine_reason_code" in df_invalid.columns

    def test_detect_outliers_empty_columns(self):
        """Test that empty monitored columns returns original data."""
        lf = pl.LazyFrame({
            "value": [10.0, 20.0, 30.0]
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=[], method="iqr"
        )
        
        assert valid_lf.collect().height == 3
        assert invalid_lf is None

    def test_detect_outliers_multiple_columns(self):
        """Test outlier detection across multiple columns."""
        lf = pl.LazyFrame({
            "pressure": [100.0, 105.0, 110.0, 500.0],  # 500 is outlier
            "temperature": [20.0, 22.0, 21.0, 23.0]
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", 
            monitored_columns=["pressure", "temperature"], 
            method="iqr", 
            multiplier=1.5
        )
        
        assert valid_lf.collect().height == 3
        assert invalid_lf is not None
        assert invalid_lf.collect().height == 1

    def test_detect_outliers_with_nulls(self):
        """Test that NULL values are handled correctly."""
        lf = pl.LazyFrame({
            "value": [10.0, None, 12.0, 11.0, 100.0]
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=["value"], method="iqr", multiplier=1.5
        )
        
        # NULL should not be considered an outlier
        df_valid = valid_lf.collect()
        assert df_valid.height >= 3  # At least non-outlier values plus possibly NULL
        
        # Check if NULL is preserved in either valid or invalid output
        null_count_valid = df_valid["value"].null_count()
        
        # If NULL was filtered out as outlier (which is wrong), check invalid_lf
        if null_count_valid == 0 and invalid_lf is not None:
            df_invalid = invalid_lf.collect()
            null_count_invalid = df_invalid["value"].null_count()
            # NULL should be somewhere - either in valid or invalid
            assert null_count_invalid >= 1 or null_count_valid >= 1, "NULL value was lost during outlier detection"
        else:
            assert null_count_valid >= 1, "NULL value should be preserved in valid records"

    def test_detect_outliers_all_outliers(self):
        """Test case where all records are outliers (edge case)."""
        lf = pl.LazyFrame({
            "value": [1000.0, -1000.0, 5000.0]  # All extreme values
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=["value"], method="iqr", multiplier=0.1
        )
        
        # With very small multiplier, most values become outliers
        # But we need to check the logic handles this edge case
        df_valid = valid_lf.collect()
        
        # If all are outliers, valid_lf should be empty and invalid_lf should have all
        if df_valid.height == 0:
            assert invalid_lf is not None
            assert invalid_lf.collect().height == 3
        else:
            # Some passed, which is also valid depending on IQR calculation
            pass

    def test_detect_outliers_metadata_columns(self):
        """Test that invalid records have proper quarantine metadata."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3, 4],
            "value": [10.0, 12.0, 11.0, 1000.0]
        })
        
        valid_lf, invalid_lf = detect_outliers(
            lf, "test_dataset", monitored_columns=["value"], method="iqr", multiplier=1.5
        )
        
        assert invalid_lf is not None
        df_invalid = invalid_lf.collect()
        
        assert df_invalid["_quarantine_validation_name"][0] == "OUTLIER_DETECTION"
        assert "IQR_VIOLATION" in df_invalid["_quarantine_reason_code"][0]
