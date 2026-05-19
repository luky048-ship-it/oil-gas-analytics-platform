"""Unit tests for pipeline_execution.py module."""

import pytest
from datetime import datetime
from plugins.bronze_to_silver.pipeline_execution import PipelineExecutionResult


class TestPipelineExecutionResult:
    """Tests for the PipelineExecutionResult dataclass."""

    def test_create_result(self):
        """Test creating a PipelineExecutionResult instance."""
        result = PipelineExecutionResult(
            dataset="production",
            partition_date="2024-01-15",
            processed_rows=1000,
            quarantined_rows=50,
            output_path="s3://datalake/silver/production/partition_date=2024-01-15",
            execution_time_sec=45.5,
            watermark=datetime(2024, 1, 15, 0, 0),
        )

        assert result.dataset == "production"
        assert result.partition_date == "2024-01-15"
        assert result.processed_rows == 1000
        assert result.quarantined_rows == 50
        assert result.output_path == "s3://datalake/silver/production/partition_date=2024-01-15"
        assert result.execution_time_sec == 45.5
        assert result.watermark == datetime(2024, 1, 15, 0, 0)

    def test_result_zero_quarantine(self):
        """Test result with no quarantined rows."""
        result = PipelineExecutionResult(
            dataset="wells",
            partition_date="2024-01-15",
            processed_rows=500,
            quarantined_rows=0,
            output_path="s3://datalake/silver/wells/partition_date=2024-01-15",
            execution_time_sec=20.0,
            watermark=datetime(2024, 1, 15, 0, 0),
        )

        assert result.quarantined_rows == 0
        assert result.processed_rows == 500

    def test_result_immutability(self):
        """Test that dataclass fields can be replaced using dataclasses.replace."""
        from dataclasses import replace
        
        result = PipelineExecutionResult(
            dataset="test",
            partition_date="2024-01-15",
            processed_rows=100,
            quarantined_rows=10,
            output_path="s3://test",
            execution_time_sec=5.0,
            watermark=datetime(2024, 1, 15),
        )

        # Create a new instance with updated values using dataclasses.replace
        updated = replace(result, processed_rows=200, quarantined_rows=5)
        
        assert updated.processed_rows == 200
        assert updated.quarantined_rows == 5
        # Original should be unchanged
        assert result.processed_rows == 100

    def test_result_repr(self):
        """Test string representation of result."""
        result = PipelineExecutionResult(
            dataset="production",
            partition_date="2024-01-15",
            processed_rows=1000,
            quarantined_rows=50,
            output_path="s3://datalake/silver/production",
            execution_time_sec=45.5,
            watermark=datetime(2024, 1, 15),
        )

        repr_str = repr(result)
        assert "PipelineExecutionResult" in repr_str
        assert "production" in repr_str
        assert "1000" in repr_str
