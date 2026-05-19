"""Unit tests for metadata_utils.py module."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call
from plugins.bronze_to_silver.pipeline_execution import PipelineExecutionResult
from plugins.bronze_to_silver.metadata_utils import (
    get_postgres_connection,
    get_last_watermark,
    update_pipeline_watermark,
    publish_pipeline_metadata,
)


class TestGetPostgresConnection:
    """Tests for the get_postgres_connection function."""

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_get_connection_default(self, mock_hook_class):
        """Test getting postgres connection with default conn_id."""
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook
        
        result = get_postgres_connection()
        
        mock_hook_class.assert_called_once_with(postgres_conn_id="postgres_default")
        mock_hook.get_conn.assert_called_once()
        assert result == mock_conn

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_get_connection_custom(self, mock_hook_class):
        """Test getting postgres connection with custom conn_id."""
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook
        
        result = get_postgres_connection(conn_id="my_postgres")
        
        mock_hook_class.assert_called_once_with(postgres_conn_id="my_postgres")
        assert result == mock_conn


class TestGetLastWatermark:
    """Tests for the get_last_watermark function."""

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_get_watermark_exists(self, mock_hook_class):
        """Test getting watermark when it exists."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (datetime(2024, 1, 15, 10, 0),)
        mock_hook_class.return_value = mock_hook
        
        result = get_last_watermark("production")
        
        mock_hook.get_first.assert_called_once()
        assert result == datetime(2024, 1, 15, 10, 0)

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_get_watermark_not_exists(self, mock_hook_class):
        """Test getting watermark when it doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = None
        mock_hook_class.return_value = mock_hook
        
        result = get_last_watermark("new_dataset")
        
        assert result is None

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_get_watermark_error(self, mock_hook_class):
        """Test handling of database errors."""
        mock_hook = MagicMock()
        mock_hook.get_first.side_effect = Exception("DB connection failed")
        mock_hook_class.return_value = mock_hook
        
        result = get_last_watermark("production")
        
        # Should return None on error
        assert result is None


class TestUpdatePipelineWatermark:
    """Tests for the update_pipeline_watermark function."""

    @patch('plugins.bronze_to_silver.metadata_utils.PostgresHook')
    def test_update_watermark_insert(self, mock_hook_class):
        """Test updating watermark (UPSERT)."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        
        watermark = datetime(2024, 1, 15, 12, 0)
        
        update_pipeline_watermark(
            dataset="production",
            watermark=watermark,
            execution_date="2024-01-15",
        )
        
        mock_hook.run.assert_called_once()
        call_args = mock_hook.run.call_args
        
        # Check query contains UPSERT logic
        query = call_args[0][0]
        assert "INSERT" in query
        assert "ON CONFLICT" in query
        assert "GREATEST" in query
        
        # Check parameters
        params = call_args[1]['parameters']
        assert params[0] == "production"
        assert params[1] == watermark


class TestPublishPipelineMetadata:
    """Tests for the publish_pipeline_metadata function."""

    @patch('plugins.bronze_to_silver.metadata_utils.get_postgres_connection')
    def test_publish_metadata_success(self, mock_get_conn):
        """Test publishing pipeline execution metadata."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn
        
        result = PipelineExecutionResult(
            dataset="production",
            partition_date="2024-01-15",
            processed_rows=1000,
            quarantined_rows=50,
            output_path="s3://datalake/silver/production",
            execution_time_sec=45.5,
            watermark=datetime(2024, 1, 15),
        )
        
        publish_pipeline_metadata(result)
        
        # Check execute_values was called
        from psycopg2.extras import execute_values
        # Verify cursor was used
        mock_cursor.__enter__.assert_called()
        mock_conn.commit.assert_called_once()

    @patch('plugins.bronze_to_silver.metadata_utils.get_postgres_connection')
    def test_publish_metadata_values(self, mock_get_conn):
        """Test that correct values are passed to insert."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn
        
        result = PipelineExecutionResult(
            dataset="test_dataset",
            partition_date="2024-01-20",
            processed_rows=500,
            quarantined_rows=10,
            output_path="s3://test",
            execution_time_sec=30.0,
            watermark=datetime(2024, 1, 20),
        )
        
        # Mock execute_values to capture arguments
        with patch('plugins.bronze_to_silver.metadata_utils.execute_values') as mock_exec:
            publish_pipeline_metadata(result)
            
            mock_exec.assert_called_once()
            call_args = mock_exec.call_args
            
            # Check values tuple
            values = call_args[0][2]
            assert len(values) == 1
            row = values[0]
            assert row[0] == "test_dataset"
            assert row[1] == "2024-01-20"
            assert row[2] == 500
            assert row[3] == 10
            assert row[4] == 30.0
            assert row[6] == "SUCCESS"
