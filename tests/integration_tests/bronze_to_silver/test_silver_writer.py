"""Unit tests for silver_writer.py module."""

import pytest
import polars as pl
from datetime import date
from unittest.mock import MagicMock, patch
from plugins.bronze_to_silver.silver_writer import write_silver_dataset


class TestWriteSilverDataset:
    """Tests for the write_silver_dataset function."""

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_success(self, mock_write, mock_get_fs):
        """Test successful silver dataset write."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        data = {
            "well_id": [1, 2, 3],
            "value": [100, 200, 300],
            "timestamp": [date(2024, 1, 15), date(2024, 1, 15), date(2024, 1, 15)],
        }
        lf = pl.LazyFrame(data)
        
        result_path = write_silver_dataset(
            lf=lf,
            dataset="production",
            partition_date="2024-01-15",
            silver_base="s3://datalake/silver",
        )
        
        assert result_path == "s3://datalake/silver/production/partition_date=2024-01-15"
        mock_write.assert_called_once()
        
        call_args = mock_write.call_args
        assert call_args[1]['base_dir'] == "datalake/silver/production"
        assert call_args[1]['format'] == "parquet"

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_with_time_column(self, mock_write, mock_get_fs):
        """Test write using event time column for partitioning."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        data = {
            "well_id": [1, 2],
            "value": [100, 200],
            "event_date": [date(2024, 1, 14), date(2024, 1, 15)],
        }
        lf = pl.LazyFrame(data)
        
        result_path = write_silver_dataset(
            lf=lf,
            dataset="production",
            partition_date="2024-01-15",
            time_column="event_date",
        )
        
        # Verify write was called
        mock_write.assert_called_once()
        
        # Get the written table to check partition_date column
        call_args = mock_write.call_args
        written_table = call_args[1]['data']
        
        # Should have partition_date column extracted from event_date
        assert "partition_date" in written_table.column_names

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_empty_dataset(self, mock_write, mock_get_fs):
        """Test write with empty dataset."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        lf = pl.LazyFrame({
            "well_id": pl.Int64(),
            "value": pl.Float64(),
        })
        
        result_path = write_silver_dataset(
            lf=lf,
            dataset="production",
            partition_date="2024-01-15",
        )
        
        assert result_path == "s3://datalake/silver/production/partition_date=2024-01-15"
        # write_dataset should not be called for empty datasets
        mock_write.assert_not_called()

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    def test_write_s3_connection_error(self, mock_get_fs):
        """Test handling of S3 connection errors."""
        mock_get_fs.side_effect = Exception("Connection failed")
        
        lf = pl.LazyFrame({"id": [1]})
        
        with pytest.raises(Exception):
            write_silver_dataset(
                lf=lf,
                dataset="production",
                partition_date="2024-01-15",
            )

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_partitioning_schema(self, mock_write, mock_get_fs):
        """Test that proper partitioning schema is used."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        lf = pl.LazyFrame({"id": [1, 2, 3]})
        
        write_silver_dataset(
            lf=lf,
            dataset="test",
            partition_date="2024-01-15",
        )
        
        call_args = mock_write.call_args
        
        # Check partitioning configuration
        assert 'partitioning' in call_args[1]
        partitioning = call_args[1]['partitioning']
        assert partitioning is not None
        
        # Check other settings
        assert call_args[1]['existing_data_behavior'] == 'overwrite_or_ignore'
        assert call_args[1]['max_partitions'] == 1024

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_custom_silver_base(self, mock_write, mock_get_fs):
        """Test write with custom silver base path."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        lf = pl.LazyFrame({"id": [1]})
        
        write_silver_dataset(
            lf=lf,
            dataset="test",
            partition_date="2024-01-15",
            silver_base="s3://custom-bucket/silver",
        )
        
        call_args = mock_write.call_args
        assert call_args[1]['base_dir'] == "custom-bucket/silver/test"

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_fallback_to_execution_date(self, mock_write, mock_get_fs):
        """Test fallback to execution_date when time_column extraction fails."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Data with wrong type for time_column
        data = {
            "well_id": [1],
            "value": [100],
            "event_date": ["not-a-date"],  # String instead of date
        }
        lf = pl.LazyFrame(data)
        
        # Should not raise, should fallback
        result_path = write_silver_dataset(
            lf=lf,
            dataset="production",
            partition_date="2024-01-15",
            time_column="event_date",
        )
        
        assert result_path == "s3://datalake/silver/production/partition_date=2024-01-15"
        mock_write.assert_called_once()

    @patch('plugins.bronze_to_silver.silver_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.silver_writer.ds.write_dataset')
    def test_write_datetime_column_conversion(self, mock_write, mock_get_fs):
        """Test write with Datetime column (should convert to date)."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        from datetime import datetime
        data = {
            "well_id": [1, 2],
            "value": [100, 200],
            "timestamp": [datetime(2024, 1, 14, 10, 0), datetime(2024, 1, 15, 15, 0)],
        }
        lf = pl.LazyFrame(data)
        
        write_silver_dataset(
            lf=lf,
            dataset="production",
            partition_date="2024-01-15",
            time_column="timestamp",
        )
        
        call_args = mock_write.call_args
        written_table = call_args[1]['data']
        
        assert "partition_date" in written_table.column_names
