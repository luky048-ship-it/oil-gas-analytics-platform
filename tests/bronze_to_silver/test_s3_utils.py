"""Unit tests for s3_utils.py module."""

import pytest
import polars as pl
from datetime import datetime, date
from unittest.mock import MagicMock, patch
from plugins.bronze_to_silver.s3_utils import get_s3_storage_options, load_bronze_dataset


class TestGetS3StorageOptions:
    """Tests for the get_s3_storage_options function."""

    @patch('plugins.bronze_to_silver.s3_utils.get_polars_storage_options')
    def test_get_storage_options_default(self, mock_get):
        """Test getting storage options with default connection."""
        mock_get.return_value = {"aws_access_key_id": "test"}
        
        result = get_s3_storage_options()
        
        mock_get.assert_called_once_with("aws_default")
        assert result == {"aws_access_key_id": "test"}

    @patch('plugins.bronze_to_silver.s3_utils.get_polars_storage_options')
    def test_get_storage_options_custom_conn(self, mock_get):
        """Test getting storage options with custom connection."""
        mock_get.return_value = {"aws_access_key_id": "custom"}
        
        result = get_s3_storage_options(conn_id="my_custom_conn")
        
        mock_get.assert_called_once_with("my_custom_conn")
        assert result == {"aws_access_key_id": "custom"}


class TestLoadBronzeDataset:
    """Tests for the load_bronze_dataset function."""

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_basic(self, mock_scan):
        """Test basic dataset loading."""
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        
        paths = ["s3://datalake/raw/production/partition_date=2024-01-15"]
        storage_options = {"aws_access_key_id": "test"}
        
        result = load_bronze_dataset(paths, storage_options)
        
        mock_scan.assert_called_once()
        call_args = mock_scan.call_args
        assert call_args[0][0] == ["s3://datalake/raw/production/partition_date=2024-01-15/*.parquet"]
        assert call_args[1]['storage_options'] == storage_options
        assert call_args[1]['hive_partitioning'] is True

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_multiple_paths(self, mock_scan):
        """Test loading from multiple paths."""
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        
        paths = [
            "s3://datalake/raw/prod/p1",
            "s3://datalake/raw/prod/p2/",
        ]
        storage_options = {}
        
        result = load_bronze_dataset(paths, storage_options)
        
        call_args = mock_scan.call_args
        # Paths should be normalized and have /*.parquet appended
        expected_paths = [
            "s3://datalake/raw/prod/p1/*.parquet",
            "s3://datalake/raw/prod/p2/*.parquet",
        ]
        assert call_args[0][0] == expected_paths

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_empty_paths(self, mock_scan):
        """Test loading with empty paths list."""
        result = load_bronze_dataset([], {})
        
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape[0] == 0
        mock_scan.assert_not_called()

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_with_watermark_datetime_column(self, mock_scan):
        """Test loading with watermark filter on Datetime column."""
        mock_lf = MagicMock()
        mock_lf.filter.return_value = mock_lf
        mock_schema = MagicMock()
        mock_schema.__getitem__ = MagicMock(return_value=pl.Datetime)
        mock_schema.__contains__ = MagicMock(return_value=True)
        mock_lf.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lf
        
        paths = ["s3://datalake/raw/prod"]
        watermark = datetime(2024, 1, 15, 10, 0)
        
        result = load_bronze_dataset(
            paths, 
            {}, 
            watermark=watermark, 
            time_column="timestamp"
        )
        
        # Verify filter was applied
        mock_lf.filter.assert_called_once()
        filter_call = mock_lf.filter.call_args
        # The filter should compare timestamp column with watermark

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_with_watermark_date_column(self, mock_scan):
        """Test loading with watermark filter on Date column."""
        mock_lf = MagicMock()
        mock_lf.filter.return_value = mock_lf
        mock_schema = MagicMock()
        mock_schema.__getitem__ = MagicMock(return_value=pl.Date)
        mock_schema.__contains__ = MagicMock(return_value=True)
        mock_lf.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lf
        
        paths = ["s3://datalake/raw/prod"]
        watermark = datetime(2024, 1, 15)
        
        result = load_bronze_dataset(
            paths, 
            {}, 
            watermark=watermark, 
            time_column="date"
        )
        
        mock_lf.filter.assert_called_once()

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_without_watermark(self, mock_scan):
        """Test loading without watermark (no filter applied)."""
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        
        paths = ["s3://datalake/raw/prod"]
        
        result = load_bronze_dataset(paths, {}, watermark=None, time_column="date")
        
        # Filter should not be called when watermark is None
        mock_lf.filter.assert_not_called()

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_without_time_column(self, mock_scan):
        """Test loading with watermark but no time_column specified."""
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        
        paths = ["s3://datalake/raw/prod"]
        watermark = datetime(2024, 1, 15)
        
        result = load_bronze_dataset(paths, {}, watermark=watermark, time_column=None)
        
        # Filter should not be called when time_column is None
        mock_lf.filter.assert_not_called()

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_scan_error(self, mock_scan):
        """Test handling of scan errors."""
        mock_scan.side_effect = Exception("S3 access denied")
        
        paths = ["s3://datalake/raw/prod"]
        
        with pytest.raises(Exception) as exc_info:
            load_bronze_dataset(paths, {})
        
        assert "S3 access denied" in str(exc_info.value)

    @patch('plugins.bronze_to_silver.s3_utils.pl.scan_parquet')
    def test_load_path_normalization(self, mock_scan):
        """Test that paths are properly normalized."""
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        
        paths = [
            "s3://bucket/data/",      # trailing slash
            "s3://bucket/data2/*.parquet",  # already has pattern
        ]
        
        result = load_bronze_dataset(paths, {})
        
        call_args = mock_scan.call_args
        expected = [
            "s3://bucket/data/*.parquet",
            "s3://bucket/data2/*.parquet",
        ]
        assert call_args[0][0] == expected
