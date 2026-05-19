"""Unit tests for config.py module."""

import pytest
import polars as pl
from plugins.bronze_to_silver.config import SCHEMA_CONTRACTS


class TestSchemaContracts:
    """Tests for the SCHEMA_CONTRACTS configuration."""

    def test_all_datasets_defined(self):
        """Test that expected datasets are defined in contracts."""
        expected_datasets = [
            "wells",
            "production",
            "well_telemetry",
            "well_targets",
            "pumps",
            "pump_sensors",
            "pump_failures",
            "deliveries",
            "drivers",
            "vehicles",
            "oil_stations",
        ]
        
        for dataset in expected_datasets:
            assert dataset in SCHEMA_CONTRACTS, f"Dataset {dataset} not found in SCHEMA_CONTRACTS"

    def test_wells_schema_structure(self):
        """Test wells dataset schema structure."""
        wells = SCHEMA_CONTRACTS["wells"]
        
        assert "columns" in wells
        assert "primary_key" in wells
        assert "time_column" in wells
        assert "dedup_key" in wells
        assert "validation_rules" in wells
        
        # Check columns
        assert "well_id" in wells["columns"]
        assert wells["columns"]["well_id"] == pl.Int32()
        assert "name" in wells["columns"]
        assert wells["columns"]["name"] == pl.String()

    def test_production_schema_structure(self):
        """Test production dataset schema structure."""
        prod = SCHEMA_CONTRACTS["production"]
        
        assert "columns" in prod
        assert "primary_key" in prod
        assert "foreign_keys" in prod
        
        # Check foreign key to wells
        assert "well_id" in prod["foreign_keys"]
        assert prod["foreign_keys"]["well_id"] == "wells.well_id"
        
        # Check validation rules
        assert "ranges" in prod["validation_rules"]
        assert "oil_ton" in prod["validation_rules"]["ranges"]
        assert prod["validation_rules"]["ranges"]["oil_ton"]["min"] == 0.0

    def test_well_telemetry_aggregation_config(self):
        """Test well_telemetry aggregation configuration."""
        telemetry = SCHEMA_CONTRACTS["well_telemetry"]
        
        assert "aggregation" in telemetry
        
        agg = telemetry["aggregation"]
        assert agg["key"] == "well_id"
        assert agg["time_column"] == "timestamp"
        assert agg["granularity"] == "1d"
        assert "metrics" in agg
        
        # Check metrics
        assert "pump_speed_rpm" in agg["metrics"]
        assert "mean" in agg["metrics"]["pump_speed_rpm"]
        assert "max" in agg["metrics"]["pump_speed_rpm"]

    def test_enum_validations_defined(self):
        """Test that enum validations are properly defined."""
        wells = SCHEMA_CONTRACTS["wells"]
        
        assert "enums" in wells["validation_rules"]
        assert "status" in wells["validation_rules"]["enums"]
        
        status_values = wells["validation_rules"]["enums"]["status"]
        assert isinstance(status_values, list)
        assert len(status_values) > 0
        assert "active" in status_values

    def test_missing_rules_defined(self):
        """Test that missing value handling rules are defined."""
        prod = SCHEMA_CONTRACTS["production"]
        
        assert "missing_rules" in prod
        
        # Check oil_ton has fill strategy
        if "oil_ton" in prod["missing_rules"]:
            rule = prod["missing_rules"]["oil_ton"]
            assert "strategy" in rule

    def test_outlier_columns_defined(self):
        """Test that outlier detection columns are defined."""
        prod = SCHEMA_CONTRACTS["production"]
        
        assert "outlier_columns" in prod
        assert isinstance(prod["outlier_columns"], list)
        
        # Should have numeric columns for outlier detection
        assert "oil_ton" in prod["outlier_columns"]
        assert "pressure" in prod["outlier_columns"]

    def test_pump_failures_enum_values(self):
        """Test pump_failures enum values."""
        failures = SCHEMA_CONTRACTS["pump_failures"]
        
        assert "enums" in failures["validation_rules"]
        assert "failure_type" in failures["validation_rules"]["enums"]
        
        failure_types = failures["validation_rules"]["enums"]["failure_type"]
        assert "electrical" in failure_types
        assert "mechanical" in failure_types
        assert "overheating" in failure_types

    def test_deliveries_schema(self):
        """Test deliveries dataset schema."""
        deliveries = SCHEMA_CONTRACTS["deliveries"]
        
        assert "columns" in deliveries
        assert "delivery_id" in deliveries["columns"]
        assert "volume_ton" in deliveries["columns"]
        assert "cost_usd" in deliveries["columns"]
        
        # Check product_type enum
        assert "enums" in deliveries["validation_rules"]
        assert "product_type" in deliveries["validation_rules"]["enums"]
        
        product_types = deliveries["validation_rules"]["enums"]["product_type"]
        assert "crude_oil" in product_types
        assert "diesel" in product_types

    def test_vehicles_fuel_type_enum(self):
        """Test vehicles fuel_type enum values."""
        vehicles = SCHEMA_CONTRACTS["vehicles"]
        
        assert "fuel_type" in vehicles["validation_rules"]["enums"]
        
        fuel_types = vehicles["validation_rules"]["enums"]["fuel_type"]
        assert "diesel" in fuel_types
        assert "electric" in fuel_types

    def test_oil_stations_geo_validation(self):
        """Test oil_stations geographic validation rules."""
        stations = SCHEMA_CONTRACTS["oil_stations"]
        
        assert "ranges" in stations["validation_rules"]
        
        ranges = stations["validation_rules"]["ranges"]
        assert "latitude" in ranges
        assert ranges["latitude"]["min"] == -90.0
        assert ranges["latitude"]["max"] == 90.0
        
        assert "longitude" in ranges
        assert ranges["longitude"]["min"] == -180.0
        assert ranges["longitude"]["max"] == 180.0

    def test_all_datasets_have_required_fields(self):
        """Test that all datasets have minimum required configuration fields."""
        required_fields = ["columns", "dedup_key"]
        
        for dataset_name, config in SCHEMA_CONTRACTS.items():
            for field in required_fields:
                assert field in config, f"Dataset {dataset_name} missing required field: {field}"
