"""
Unit tests for bronze_to_silver.deduplicator module.
Tests deduplication logic based on key columns and timestamps.
"""
import pytest
import polars as pl
import sys
sys.path.insert(0, '/workspace/plugins')
from bronze_to_silver.deduplicator import deduplicate_dataset


class TestDeduplicateDataset:
    """Test suite for deduplicate_dataset function."""

    def test_deduplicate_with_timestamp(self):
        """Test deduplication keeping the latest record by timestamp."""
        lf = pl.LazyFrame({
            "id": [1, 1, 2, 2],
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"],
            "value": [10, 20, 30, 40]
        })
        
        result = deduplicate_dataset(lf, key_columns=["id"], timestamp_column="timestamp")
        df = result.collect()
        
        assert df.height == 2
        # Should keep the latest timestamp for each id
        values = df.sort("id")["value"].to_list()
        assert values == [20, 40]  # id=1 -> 20 (Jan 2), id=2 -> 40 (Jan 3)

    def test_deduplicate_without_timestamp(self):
        """Test deduplication without timestamp - keeps last occurrence."""
        lf = pl.LazyFrame({
            "id": [1, 1, 1],
            "value": [10, 20, 30]
        })
        
        result = deduplicate_dataset(lf, key_columns=["id"], timestamp_column=None)
        df = result.collect()
        
        assert df.height == 1
        assert df["value"].item() == 30  # Last occurrence

    def test_deduplicate_empty_key_columns(self):
        """Test that empty key columns returns original dataframe."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })
        
        result = deduplicate_dataset(lf, key_columns=[], timestamp_column=None)
        df = result.collect()
        
        assert df.height == 3  # No deduplication

    def test_deduplicate_no_duplicates(self):
        """Test deduplication when there are no duplicates."""
        lf = pl.LazyFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })
        
        result = deduplicate_dataset(lf, key_columns=["id"], timestamp_column=None)
        df = result.collect()
        
        assert df.height == 3

    def test_deduplicate_composite_key(self):
        """Test deduplication with composite key."""
        lf = pl.LazyFrame({
            "id1": [1, 1, 1, 2],
            "id2": ["a", "a", "b", "a"],
            "value": [10, 20, 30, 40]
        })
        
        result = deduplicate_dataset(lf, key_columns=["id1", "id2"], timestamp_column=None)
        df = result.collect()
        
        assert df.height == 3  # (1,a), (1,b), (2,a)
        
    def test_deduplicate_with_null_keys(self):
        """Test deduplication handling NULL keys."""
        lf = pl.LazyFrame({
            "id": [1, None, 1, None],
            "value": [10, 20, 30, 40]
        })
        
        result = deduplicate_dataset(lf, key_columns=["id"], timestamp_column=None)
        df = result.collect()
        
        # NULLs are treated as a group
        assert df.height == 2
