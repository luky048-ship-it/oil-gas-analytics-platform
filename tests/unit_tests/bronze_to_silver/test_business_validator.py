"""
Unit tests for bronze_to_silver.business_validator module.
Tests business rule validation including ranges, enums, and custom rules.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.business_validator import validate_critical_rules


class TestBusinessValidator:
    """Test suite for validate_critical_rules function."""

    def test_validate_ranges_valid(self):
        """Test that valid range values pass through."""
        lf = pl.LazyFrame({
            "pressure": [100.0, 200.0, 500.0],
            "temperature": [-10.0, 0.0, 50.0]
        })
        
        rules = {
            "ranges": {
                "pressure": {"min": 0.0, "max": 1000.0},
                "temperature": {"min": -60.0, "max": 250.0}
            }
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        assert valid_lf.collect().height == 3
        assert invalid_lf is None

    def test_validate_ranges_invalid(self):
        """Test that out-of-range values are filtered to invalid."""
        lf = pl.LazyFrame({
            "pressure": [100.0, 1500.0, 500.0],  # 1500 > max
            "temperature": [-10.0, -100.0, 50.0]  # -100 < min
        })
        
        rules = {
            "ranges": {
                "pressure": {"min": 0.0, "max": 1000.0},
                "temperature": {"min": -60.0, "max": 250.0}
            }
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        assert valid_lf.collect().height == 2  # Row 0 and 2 are valid
        assert invalid_lf is not None
        assert invalid_lf.collect().height == 1  # Row 1 is invalid (both violations)

    def test_validate_enums_valid(self):
        """Test that valid enum values pass through."""
        lf = pl.LazyFrame({
            "status": ["active", "inactive", "maintenance"]
        })
        
        rules = {
            "enums": {
                "status": ["active", "inactive", "maintenance", "decommissioned"]
            }
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        assert valid_lf.collect().height == 3
        assert invalid_lf is None

    def test_validate_enums_invalid(self):
        """Test that invalid enum values are filtered."""
        lf = pl.LazyFrame({
            "status": ["active", "unknown_status", "inactive"]
        })
        
        rules = {
            "enums": {
                "status": ["active", "inactive", "maintenance"]
            }
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        assert valid_lf.collect().height == 2
        assert invalid_lf is not None
        assert invalid_lf.collect().height == 1

    def test_validate_custom_rules(self):
        """Test custom SQL-like rules."""
        lf = pl.LazyFrame({
            "pressure_in": [100.0, 200.0, 300.0],
            "pressure_out": [150.0, 180.0, 350.0]  # Row 1: 180 < 200 (violation)
        })
        
        rules = {
            "custom": [
                {"rule": "pressure_out >= pressure_in", "severity": "MEDIUM"}
            ]
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        assert valid_lf.collect().height == 2  # Rows 0 and 2
        assert invalid_lf is not None
        assert invalid_lf.collect().height == 1  # Row 1

    def test_validate_empty_rules(self):
        """Test that empty rules return original data as valid."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })
        
        valid_lf, invalid_lf = validate_critical_rules(lf, {})
        
        assert valid_lf.collect().height == 3
        assert invalid_lf is None

    def test_validate_combined_rules(self):
        """Test combination of ranges, enums, and custom rules."""
        lf = pl.LazyFrame({
            "status": ["active", "active", "unknown"],
            "pressure": [100.0, 1500.0, 500.0],
            "temperature": [25.0, 30.0, 35.0]
        })
        
        rules = {
            "enums": {
                "status": ["active", "inactive"]
            },
            "ranges": {
                "pressure": {"min": 0.0, "max": 1000.0}
            },
            "custom": [
                {"rule": "temperature > 0", "severity": "LOW"}
            ]
        }
        
        valid_lf, invalid_lf = validate_critical_rules(lf, rules)
        
        df_valid = valid_lf.collect()
        assert df_valid.height == 1  # Only row 0 passes all rules
        
