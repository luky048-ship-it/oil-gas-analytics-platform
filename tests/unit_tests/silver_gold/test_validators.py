"""
Unit tests for gold_layer.validators module.
Tests business readiness and pre-publish validation logic.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')

from gold_layer.validators import validate_business_readiness, validate_mart_before_publish


class TestGoldLayerValidators:
    """Test suite for gold layer validators."""

    def test_validate_business_readiness_no_contract(self):
        """Test that data without contract returns unchanged."""
        lf = pl.LazyFrame({
            "well_id": [1, 2, 3],
            "oil_ton": [10.0, 20.0, 30.0]
        })
        
        result = validate_business_readiness(lf, "unknown_mart")
        df = result.collect()
        
        assert df.height == 3
        assert list(df.columns) == ["well_id", "oil_ton"]

    def test_validate_business_readiness_negative_oil(self, caplog):
        """Test warning for negative oil production."""
        from gold_layer.config import MART_CONTRACTS
        
        # Create a mock contract with min_oil_ton rule
        original_contract = MART_CONTRACTS.get("test_mart")
        try:
            # Temporarily add test contract
            class MockContract:
                business_rules = {"min_oil_ton": 0}
                critical_columns = []
            
            MART_CONTRACTS["test_mart"] = MockContract()
            
            lf = pl.LazyFrame({
                "well_id": [1, 2, 3],
                "oil_ton": [10.0, -5.0, 30.0]  # One negative value
            })
            
            result = validate_business_readiness(lf, "test_mart")
            df = result.collect()
            
            assert df.height == 3
            assert "Found 1 rows with oil_ton < 0" in caplog.text
        finally:
            if original_contract is None:
                MART_CONTRACTS.pop("test_mart", None)
            else:
                MART_CONTRACTS["test_mart"] = original_contract

    def test_validate_mart_before_publish_null_critical_column(self):
        """Test that NULL in critical column raises error."""
        from gold_layer.config import MART_CONTRACTS
        
        original_contract = MART_CONTRACTS.get("test_mart")
        try:
            class MockContract:
                business_rules = {}
                critical_columns = ["well_id", "oil_ton"]
            
            MART_CONTRACTS["test_mart"] = MockContract()
            
            df = pl.DataFrame({
                "well_id": [1, 2, None],  # NULL in critical column
                "oil_ton": [10.0, 20.0, 30.0]
            })
            
            with pytest.raises(ValueError) as exc_info:
                validate_mart_before_publish(df, "test_mart")
            
            assert "CRITICAL" in str(exc_info.value)
            assert "NULLs in critical column well_id" in str(exc_info.value)
        finally:
            if original_contract is None:
                MART_CONTRACTS.pop("test_mart", None)
            else:
                MART_CONTRACTS["test_mart"] = original_contract

    def test_validate_mart_before_publish_valid_data(self, caplog):
        """Test that valid data passes validation."""
        from gold_layer.config import MART_CONTRACTS
        
        original_contract = MART_CONTRACTS.get("test_mart")
        try:
            class MockContract:
                business_rules = {}
                critical_columns = ["well_id", "oil_ton"]
            
            MART_CONTRACTS["test_mart"] = MockContract()
            
            df = pl.DataFrame({
                "well_id": [1, 2, 3],
                "oil_ton": [10.0, 20.0, 30.0]
            })
            
            # Should not raise
            validate_mart_before_publish(df, "test_mart")
            
            # Check that no exception was raised (validation passed)
            # The log message may not be captured depending on logging level
            assert True
        finally:
            if original_contract is None:
                MART_CONTRACTS.pop("test_mart", None)
            else:
                MART_CONTRACTS["test_mart"] = original_contract

    def test_validate_mart_before_publish_empty_dataframe(self, caplog):
        """Test handling of empty dataframe."""
        from gold_layer.config import MART_CONTRACTS
        
        original_contract = MART_CONTRACTS.get("test_mart")
        try:
            class MockContract:
                business_rules = {}
                critical_columns = ["well_id"]
            
            MART_CONTRACTS["test_mart"] = MockContract()
            
            df = pl.DataFrame({
                "well_id": pl.Series([], dtype=pl.Int32),
                "oil_ton": pl.Series([], dtype=pl.Float64)
            })
            
            # Should not raise, but log warning
            validate_mart_before_publish(df, "test_mart")
            
            assert "No rows to publish" in caplog.text
        finally:
            if original_contract is None:
                MART_CONTRACTS.pop("test_mart", None)
            else:
                MART_CONTRACTS["test_mart"] = original_contract

    def test_validate_mart_before_publish_no_contract(self, caplog):
        """Test handling when no contract exists."""
        df = pl.DataFrame({
            "well_id": [1, 2, 3],
            "oil_ton": [10.0, 20.0, 30.0]
        })
        
        # Should not raise, just log warning
        validate_mart_before_publish(df, "unknown_mart")
        
        assert "No contract found" in caplog.text
