"""Unit tests for quarantine_writer.py module."""

import pytest
import polars as pl
from datetime import datetime
from unittest.mock import MagicMock, patch, call
from airflow.exceptions import AirflowException
from plugins.bronze_to_silver.quarantine_writer import write_quarantine_dataset


class TestWriteQuarantineDataset:
    """Tests for the write_quarantine_dataset function."""

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_success(self, mock_write, mock_get_fs):
        """Test successful quarantine write."""
        # Mock S3 filesystem
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Create test data with invalid records
        invalid_data = {
            "well_id": [1, 2],
            "value": [-100, -200],  # Invalid negative values
            "error_reason": ["negative_value", "negative_value"],
        }
        invalid_lf = pl.LazyFrame(invalid_data)
        
        # Execute
        result = write_quarantine_dataset(
            invalid_lf=invalid_lf,
            dataset="production",
            reason_code="negative_value",
            execution_date="2024-01-15",
            base_path="s3://datalake/quarantine",
        )
        
        # Verify
        assert result == 2  # 2 rows quarantined
        mock_write.assert_called_once()
        
        # Check call arguments
        call_args = mock_write.call_args
        assert call_args[1]['base_dir'] == "datalake/quarantine/production"
        assert call_args[1]['format'] == "parquet"

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_empty(self, mock_write, mock_get_fs):
        """Test quarantine write with no invalid records."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        # Empty lazy frame
        invalid_lf = pl.LazyFrame({
            "well_id": pl.Int64(),
            "value": pl.Float64(),
        })
        
        result = write_quarantine_dataset(
            invalid_lf=invalid_lf,
            dataset="production",
            reason_code="schema_violation",
            execution_date="2024-01-15",
        )
        
        # Should return 0 and not call write_dataset
        assert result == 0
        mock_write.assert_not_called()

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    def test_write_quarantine_s3_connection_error(self, mock_get_fs):
        """Test handling of S3 connection errors."""
        mock_get_fs.side_effect = Exception("S3 connection failed")
        
        invalid_lf = pl.LazyFrame({"id": [1, 2]})
        
        with pytest.raises(AirflowException) as exc_info:
            write_quarantine_dataset(
                invalid_lf=invalid_lf,
                dataset="production",
                reason_code="test",
                execution_date="2024-01-15",
            )
        
        assert "Could not connect to S3" in str(exc_info.value)

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_enrichment_columns(self, mock_write, mock_get_fs):
        """Test that quarantine adds metadata columns."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        invalid_data = {"well_id": [1], "value": [-100]}
        invalid_lf = pl.LazyFrame(invalid_data)
        
        write_quarantine_dataset(
            invalid_lf=invalid_lf,
            dataset="production",
            reason_code="negative_value",
            execution_date="2024-01-15",
        )
        
        # Get the table that was written
        call_args = mock_write.call_args
        written_table = call_args[1]['data']
        
        # Check metadata columns were added
        assert "_quarantine_execution_date" in written_table.column_names
        assert "_quarantine_source_dataset" in written_table.column_names
        assert "partition_date" in written_table.column_names
        
        # Check values
        dataset_col = written_table.column("_quarantine_source_dataset")
        assert dataset_col[0].as_py() == "production"

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_partitioning(self, mock_write, mock_get_fs):
        """Test that quarantine uses proper partitioning."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        invalid_lf = pl.LazyFrame({"id": [1, 2, 3]})
        
        write_quarantine_dataset(
            invalid_lf=invalid_lf,
            dataset="wells",
            reason_code="null_key",
            execution_date="2024-01-15",
        )
        
        call_args = mock_write.call_args
        
        # Check partitioning schema
        partitioning = call_args[1]['partitioning']
        assert partitioning is not None
        
        # Check existing_data_behavior
        assert call_args[1]['existing_data_behavior'] == 'overwrite_or_ignore'

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_write_error(self, mock_write, mock_get_fs):
        """Test handling of write errors."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        mock_write.side_effect = Exception("IO Error")
        
        invalid_lf = pl.LazyFrame({"id": [1]})
        
        with pytest.raises(AirflowException) as exc_info:
            write_quarantine_dataset(
                invalid_lf=invalid_lf,
                dataset="production",
                reason_code="test",
                execution_date="2024-01-15",
            )
        
        assert "IO Error writing to quarantine" in str(exc_info.value)

    @patch('plugins.bronze_to_silver.quarantine_writer.get_s3_filesystem')
    @patch('plugins.bronze_to_silver.quarantine_writer.ds.write_dataset')
    def test_write_quarantine_custom_base_path(self, mock_write, mock_get_fs):
        """Test quarantine with custom base path."""
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        
        invalid_lf = pl.LazyFrame({"id": [1]})
        
        write_quarantine_dataset(
            invalid_lf=invalid_lf,
            dataset="test",
            reason_code="test",
            execution_date="2024-01-15",
            base_path="s3://custom-bucket/quarantine",
        )
        
        call_args = mock_write.call_args
        assert call_args[1]['base_dir'] == "custom-bucket/quarantine/test"
