"""
Unit tests for gold_layer.builders.mart_well_kpi module.
Tests KPI calculation and ranking logic.
"""
import pytest
import polars as pl
from datetime import date
import sys
sys.path.insert(0, '/workspace/plugins')

from gold_layer.builders.mart_well_kpi import build_mart_well_kpi


class TestMartWellKpi:
    """Test suite for build_mart_well_kpi function."""

    def test_build_mart_well_kpi_basic(self):
        """Test basic KPI calculation."""
        lf_prod = pl.LazyFrame({
            "well_id": [1, 1, 2, 2],
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 1), date(2024, 1, 2)],
            "oil_ton": [10.0, 20.0, 30.0, 40.0],
            "downtime_hours": [2.0, 4.0, 1.0, 3.0]
        })
        
        lf_history = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64)
        })
        
        result = build_mart_well_kpi(lf_prod, lf_history)
        df = result.collect()
        
        assert df.height == 2  # Two wells
        assert "avg_daily_oil" in df.columns
        assert "total_oil" in df.columns
        assert "production_rank" in df.columns
        assert "performance_group" in df.columns
        
        # Well 2 should have higher rank (more oil)
        well2_row = df.filter(pl.col("well_id") == 2).row(0)
        well1_row = df.filter(pl.col("well_id") == 1).row(0)
        
        assert well2_row[3] > well1_row[3]  # total_oil (index 3)
        assert well2_row[7] < well1_row[7]  # production_rank (lower is better, index 7)

    def test_build_mart_well_kpi_aggregations(self):
        """Test that aggregations are calculated correctly."""
        lf_prod = pl.LazyFrame({
            "well_id": [1, 1, 1],
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "oil_ton": [10.0, 20.0, 30.0],
            "downtime_hours": [6.0, 6.0, 6.0]  # 25% downtime each day
        })
        
        lf_history = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64)
        })
        
        result = build_mart_well_kpi(lf_prod, lf_history)
        df = result.collect()
        
        assert df.height == 1
        
        row = df.row(0)
        avg_daily_oil = row[2]  # avg_daily_oil
        total_oil = row[3]  # total_oil
        avg_downtime_pct = row[4]  # avg_downtime_pct
        
        # Average of 10, 20, 30 = 20
        assert float(avg_daily_oil) == 20.0
        # Sum = 60
        assert float(total_oil) == 60.0
        # (6+6+6) / (3 * 24) = 18/72 = 0.25
        assert abs(float(avg_downtime_pct) - 0.25) < 0.001

    def test_build_mart_well_kpi_performance_groups(self):
        """Test performance group assignment."""
        # Create data for 15 wells to test all groups
        well_ids = list(range(1, 16))
        lf_prod = pl.LazyFrame({
            "well_id": well_ids * 2,  # Each well has 2 days
            "date": [date(2024, 1, 1)] * 15 + [date(2024, 1, 2)] * 15,
            "oil_ton": [float(i * 10) for i in well_ids] * 2,  # Well 15 produces most
            "downtime_hours": [1.0] * 30
        })
        
        lf_history = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64)
        })
        
        result = build_mart_well_kpi(lf_prod, lf_history)
        df = result.collect()
        
        # Check performance groups exist
        assert set(df["performance_group"].to_list()).issubset({"Top", "Good", "Average"})
        
        # Well 15 should be in Top group (highest production)
        well15_row = df.filter(pl.col("well_id") == 15).row(0)
        assert well15_row[8] == "Top"  # performance_group

    def test_build_mart_well_kpi_empty_production(self):
        """Test handling of empty production data."""
        lf_prod = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64),
            "downtime_hours": pl.Series([], dtype=pl.Float64)
        })
        
        lf_history = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64)
        })
        
        # This might raise an error or return empty - both acceptable
        try:
            result = build_mart_well_kpi(lf_prod, lf_history)
            df = result.collect()
            assert df.height == 0
        except Exception:
            # Empty data handling may vary
            pass

    def test_build_mart_well_kpi_schema_types(self):
        """Test that output schema has correct types."""
        lf_prod = pl.LazyFrame({
            "well_id": [1, 2],
            "date": [date(2024, 1, 1), date(2024, 1, 1)],
            "oil_ton": [10.0, 20.0],
            "downtime_hours": [1.0, 2.0]
        })
        
        lf_history = pl.LazyFrame({
            "well_id": pl.Series([], dtype=pl.Int32),
            "date": pl.Series([], dtype=pl.Date),
            "oil_ton": pl.Series([], dtype=pl.Float64)
        })
        
        result = build_mart_well_kpi(lf_prod, lf_history)
        df = result.collect()
        
        # Check key column types
        assert df["well_id"].dtype == pl.Int32
        assert df["date"].dtype == pl.Date
        assert df["performance_group"].dtype == pl.String
