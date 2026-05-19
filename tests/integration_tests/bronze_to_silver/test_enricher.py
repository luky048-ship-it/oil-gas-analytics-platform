"""Unit tests for enricher.py module."""

import polars as pl
import pytest
from plugins.bronze_to_silver.enricher import enrich_reference_data


class TestEnrichReferenceData:
    """Tests for the enrich_reference_data function."""

    def test_enrich_basic_left_join(self, tmp_path):
        """Test basic left join with reference data."""
        # Create test data
        main_data = {
            "well_id": [1, 2, 3],
            "value": [100, 200, 300],
        }
        ref_data = {
            "well_id": [1, 2, 4],
            "name": ["Well A", "Well B", "Well D"],
            "region": ["North", "South", "East"],
        }

        # Write reference data to temp parquet file
        ref_df = pl.DataFrame(ref_data)
        ref_path = tmp_path / "reference"
        ref_path.mkdir()
        ref_df.write_parquet(ref_path / "ref.parquet")

        # Create main lazy frame
        main_lf = pl.LazyFrame(main_data)

        # Enrich
        storage_options = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        enriched_lf = enrich_reference_data(
            lf=main_lf,
            reference_dataset=str(ref_path),
            join_key="well_id",
            storage_options=storage_options,
            how="left",
        )

        result = enriched_lf.collect()

        # Verify results
        assert result.shape[0] == 3  # All rows from main preserved
        assert "name" in result.columns
        assert "region" in result.columns
        
        # Check specific values
        well_1 = result.filter(pl.col("well_id") == 1)
        assert well_1["name"][0] == "Well A"
        assert well_1["region"][0] == "North"
        
        # Well 3 should have nulls (no match in reference)
        well_3 = result.filter(pl.col("well_id") == 3)
        assert well_3["name"][0] is None

    def test_enrich_inner_join(self, tmp_path):
        """Test inner join - only matching rows."""
        main_data = {"well_id": [1, 2, 3], "value": [100, 200, 300]}
        ref_data = {"well_id": [1, 2, 4], "name": ["A", "B", "D"]}

        ref_df = pl.DataFrame(ref_data)
        ref_path = tmp_path / "reference"
        ref_path.mkdir()
        ref_df.write_parquet(ref_path / "ref.parquet")

        main_lf = pl.LazyFrame(main_data)

        storage_options = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        enriched_lf = enrich_reference_data(
            lf=main_lf,
            reference_dataset=str(ref_path),
            join_key="well_id",
            storage_options=storage_options,
            how="inner",
        )

        result = enriched_lf.collect()

        # Only 2 rows should match (well_id 1 and 2)
        assert result.shape[0] == 2
        assert set(result["well_id"].to_list()) == {1, 2}

    def test_enrich_empty_main(self, tmp_path):
        """Test enrichment when main dataset is empty."""
        main_data = {"well_id": [], "value": []}
        ref_data = {"well_id": [1, 2], "name": ["A", "B"]}

        ref_df = pl.DataFrame(ref_data)
        ref_path = tmp_path / "reference"
        ref_path.mkdir()
        ref_df.write_parquet(ref_path / "ref.parquet")

        main_lf = pl.LazyFrame(main_data, schema={"well_id": pl.Int64, "value": pl.Int64})

        storage_options = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        enriched_lf = enrich_reference_data(
            lf=main_lf,
            reference_dataset=str(ref_path),
            join_key="well_id",
            storage_options=storage_options,
        )

        result = enriched_lf.collect()
        assert result.shape[0] == 0

    def test_enrich_multiple_columns(self, tmp_path):
        """Test enrichment with multiple columns from reference."""
        main_data = {"id": [1, 2], "metric": [10, 20]}
        ref_data = {
            "id": [1, 2],
            "attr1": ["X", "Y"],
            "attr2": [100, 200],
            "attr3": ["P", "Q"],
        }

        ref_df = pl.DataFrame(ref_data)
        ref_path = tmp_path / "reference"
        ref_path.mkdir()
        ref_df.write_parquet(ref_path / "ref.parquet")

        main_lf = pl.LazyFrame(main_data)

        storage_options = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        enriched_lf = enrich_reference_data(
            lf=main_lf,
            reference_dataset=str(ref_path),
            join_key="id",
            storage_options=storage_options,
        )

        result = enriched_lf.collect()

        assert "attr1" in result.columns
        assert "attr2" in result.columns
        assert "attr3" in result.columns
        assert result["attr1"].to_list() == ["X", "Y"]
        assert result["attr2"].to_list() == [100, 200]
