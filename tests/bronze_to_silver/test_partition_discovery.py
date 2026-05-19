"""Unit tests for partition_discovery.py module."""

import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch
from plugins.bronze_to_silver.partition_discovery import discover_incremental_partitions


class TestDiscoverIncrementalPartitions:
    """Tests for the discover_incremental_partitions function."""

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_with_watermark(self, mock_get_fs):
        """Test partition discovery with watermark filtering."""
        # Mock S3 filesystem
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Mock file paths
        mock_files = [
            "datalake/raw/production/partition_date=2024-01-01/data.parquet",
            "datalake/raw/production/partition_date=2024-01-02/data.parquet",
            "datalake/raw/production/partition_date=2024-01-03/data.parquet",
        ]
        mock_fs.glob.return_value = mock_files
        
        # Watermark is 2024-01-02, so should return partitions >= that date
        watermark = datetime(2024, 1, 2)
        
        result = discover_incremental_partitions(
            dataset="production",
            watermark=watermark,
            storage_options={},
            bronze_base="s3://datalake/raw"
        )
        
        # Should return 2 partitions (Jan 2 and Jan 3)
        assert len(result) == 2
        assert "s3://datalake/raw/production/partition_date=2024-01-02" in result
        assert "s3://datalake/raw/production/partition_date=2024-01-03" in result
        assert "s3://datalake/raw/production/partition_date=2024-01-01" not in result

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_no_watermark(self, mock_get_fs):
        """Test partition discovery without watermark (all partitions)."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        mock_files = [
            "datalake/raw/wells/partition_date=2024-01-01/data.parquet",
            "datalake/raw/wells/partition_date=2024-01-02/data.parquet",
        ]
        mock_fs.glob.return_value = mock_files
        
        result = discover_incremental_partitions(
            dataset="wells",
            watermark=None,
            storage_options={},
            bronze_base="s3://datalake/raw"
        )
        
        # Should return all partitions
        assert len(result) == 2
        assert "s3://datalake/raw/wells/partition_date=2024-01-01" in result
        assert "s3://datalake/raw/wells/partition_date=2024-01-02" in result

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_no_partition_date_in_path(self, mock_get_fs):
        """Test files without partition_date in path."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Files without partition_date
        mock_files = [
            "datalake/raw/static/data.parquet",
        ]
        mock_fs.glob.return_value = mock_files
        
        result = discover_incremental_partitions(
            dataset="static",
            watermark=None,
            storage_options={},
            bronze_base="s3://datalake/raw"
        )
        
        # Should return base path
        assert len(result) == 1
        assert "s3://datalake/raw/static" in result

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_s3_connection_error(self, mock_get_fs):
        """Test handling of S3 connection errors."""
        mock_get_fs.side_effect = Exception("Connection failed")
        
        result = discover_incremental_partitions(
            dataset="production",
            watermark=None,
            storage_options={},
        )
        
        # Should return empty list on error
        assert result == []

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_no_files_found(self, mock_get_fs):
        """Test when no files are found in S3."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        mock_fs.glob.return_value = []
        
        result = discover_incremental_partitions(
            dataset="empty_dataset",
            watermark=None,
            storage_options={},
        )
        
        assert result == []

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_invalid_partition_date_format(self, mock_get_fs):
        """Test handling of invalid partition date formats."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        mock_files = [
            "datalake/raw/test/partition_date=invalid-date/data.parquet",
            "datalake/raw/test/partition_date=2024-01-01/data.parquet",
        ]
        mock_fs.glob.return_value = mock_files
        
        result = discover_incremental_partitions(
            dataset="test",
            watermark=None,
            storage_options={},
        )
        
        # Should skip invalid and return valid one
        assert len(result) == 1
        assert "s3://datalake/raw/test/partition_date=2024-01-01" in result

    @patch('plugins.bronze_to_silver.partition_discovery.get_s3_filesystem')
    def test_discover_duplicate_partitions(self, mock_get_fs):
        """Test that duplicate partitions are deduplicated."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Multiple files in same partition
        mock_files = [
            "datalake/raw/prod/partition_date=2024-01-01/file1.parquet",
            "datalake/raw/prod/partition_date=2024-01-01/file2.parquet",
            "datalake/raw/prod/partition_date=2024-01-01/file3.parquet",
        ]
        mock_fs.glob.return_value = mock_files
        
        result = discover_incremental_partitions(
            dataset="prod",
            watermark=None,
            storage_options={},
        )
        
        # Should return only one partition path
        assert len(result) == 1
        assert "s3://datalake/raw/prod/partition_date=2024-01-01" in result
