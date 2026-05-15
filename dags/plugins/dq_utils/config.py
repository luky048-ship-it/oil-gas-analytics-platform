from __future__ import annotations
import polars as pl
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

@dataclass
class ForeignKeyContract:
    column: str
    parent_table: str
    parent_column: str

@dataclass
class TableContract:
    schema: Dict[str, pl.DataType]
    primary_keys: List[str]
    not_null_columns: List[str]
    foreign_keys: List[ForeignKeyContract] = field(default_factory=list)
    unique_columns: List[str] = field(default_factory=list)
    value_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = field(default_factory=dict)
    enums: Dict[str, List[str]] = field(default_factory=dict)
    custom_rules: List[str] = field(default_factory=list)
    freshness_sla_minutes: Optional[int] = None
    partition_column: Optional[str] = None
    statistical_monitored_columns: List[str] = field(default_factory=list)

APPROVED_WELL_STATUSES = ["active", "inactive", "maintenance", "decommissioned"]
APPROVED_FAILURE_TYPES = ["electrical", "mechanical", "overheating", "seal_failure", "vibration_alarm", "pressure_loss", "unknown"]
APPROVED_PRODUCT_TYPES = ["crude_oil", "condensate", "diesel", "drilling_fluids"]
APPROVED_FUEL_TYPES = ["diesel", "gasoline", "electric", "hybrid", "lng"]

TABLE_CONTRACTS: Dict[str, TableContract] = {
    "wells": TableContract(
        schema={
            "well_id": pl.Int32(),
            "name": pl.String(),
            "field_name": pl.String(),
            "region": pl.String(),
            "start_date": pl.Date(),
            "operator": pl.String(),
            "status": pl.String(),
        },
        primary_keys=["well_id"],
        not_null_columns=["well_id", "name", "field_name", "region", "start_date", "status"],
        enums={"status": APPROVED_WELL_STATUSES},
    ),
    "production": TableContract(
        schema={
            "prod_id": pl.Int32(),
            "well_id": pl.Int32(),
            "date": pl.Date(),
            "oil_ton": pl.Float64(),
            "gas_m3": pl.Float64(),
            "water_m3": pl.Float64(),
            "energy_kwh": pl.Float64(),
            "downtime_hours": pl.Float64(),
            "temperature": pl.Float64(),
            "pressure": pl.Float64(),
        },
        primary_keys=["prod_id"],
        not_null_columns=["prod_id", "well_id", "date"],
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
        value_ranges={
            "oil_ton": (0.0, None),
            "gas_m3": (0.0, None),
            "water_m3": (0.0, None),
            "energy_kwh": (0.0, None),
            "downtime_hours": (0.0, 24.0),
            "pressure": (0.0, 1000.0),
            "temperature": (-60.0, 250.0),
        },
    ),
    "well_telemetry": TableContract(
        schema={
            "record_id": pl.Int32(),
            "well_id": pl.Int32(),
            "timestamp": pl.Datetime("ms"),
            "pump_speed_rpm": pl.Float64(),
            "pump_current": pl.Float64(),
            "pressure_in": pl.Float64(),
            "pressure_out": pl.Float64(),
            "temperature": pl.Float64(),
            "vibration": pl.Float64(),
            "oil_flow_rate": pl.Float64(),
        },
        primary_keys=["record_id"],
        not_null_columns=["record_id", "well_id", "timestamp"],
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
        value_ranges={
            "pump_speed_rpm": (0.0, None),
            "vibration": (0.0, None),
            "oil_flow_rate": (0.0, None),
        },
    ),
    "well_targets": TableContract(
        schema={
            "well_id": pl.Int32(),
            "date": pl.Date(),
            "daily_oil_ton": pl.Float64(),
        },
        primary_keys=["well_id", "date"],
        not_null_columns=["well_id", "date", "daily_oil_ton"],
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
        value_ranges={"daily_oil_ton": (0.0, None)},
    ),
    "pumps": TableContract(
        schema={
            "pump_id": pl.Int32(),
            "well_id": pl.Int32(),
            "type": pl.String(),
            "install_date": pl.Date(),
            "manufacturer": pl.String(),
            "model": pl.String(),
        },
        primary_keys=["pump_id"],
        not_null_columns=["pump_id", "well_id", "type", "install_date"],
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
    ),
    "pump_sensors": TableContract(
        schema={
            "record_id": pl.Int32(),
            "pump_id": pl.Int32(),
            "timestamp": pl.Datetime("ms"),
            "temperature": pl.Float64(),
            "vibration": pl.Float64(),
            "current": pl.Float64(),
            "rpm": pl.Float64(),
            "pressure": pl.Float64(),
        },
        primary_keys=["record_id"],
        not_null_columns=["record_id", "pump_id", "timestamp"],
        foreign_keys=[ForeignKeyContract("pump_id", "pumps", "pump_id")],
        value_ranges={
            "vibration": (0.0, None),
            "rpm": (0.0, None),
            "pressure": (0.0, None),
        },
    ),
    "pump_failures": TableContract(
        schema={
            "failure_id": pl.Int32(),
            "pump_id": pl.Int32(),
            "failure_date": pl.Datetime("ms"),
            "failure_type": pl.String(),
            "downtime_hours": pl.Float64(),
        },
        primary_keys=["failure_id"],
        not_null_columns=["failure_id", "pump_id", "failure_date", "failure_type"],
        foreign_keys=[ForeignKeyContract("pump_id", "pumps", "pump_id")],
        value_ranges={"downtime_hours": (0.0, None)},
        enums={"failure_type": APPROVED_FAILURE_TYPES},
    ),
    "deliveries": TableContract(
        schema={
            "delivery_id": pl.Int32(),
            "date": pl.Date(),
            "source": pl.String(),
            "destination": pl.String(),
            "product_type": pl.String(),
            "volume_ton": pl.Float64(),
            "cost_usd": pl.Float64(),
            "delay_hours": pl.Float64(),
            "distance_km": pl.Float64(),
            "weather_conditions": pl.String(),
            "driver_id": pl.Int32(),
            "vehicle_id": pl.Int32(),
        },
        primary_keys=["delivery_id"],
        not_null_columns=["delivery_id", "date", "source", "destination", "product_type", "driver_id", "vehicle_id"],
        foreign_keys=[
            ForeignKeyContract("driver_id", "drivers", "driver_id"),
            ForeignKeyContract("vehicle_id", "vehicles", "vehicle_id"),
        ],
        value_ranges={
            "volume_ton": (0.0, None),
            "cost_usd": (0.0, None),
            "delay_hours": (0.0, None),
            "distance_km": (0.0001, None),
        },
        enums={"product_type": APPROVED_PRODUCT_TYPES},
    ),
    "drivers": TableContract(
        schema={
            "driver_id": pl.Int32(),
            "name": pl.String(),
            "experience_years": pl.Int32(),
            "region": pl.String(),
        },
        primary_keys=["driver_id"],
        not_null_columns=["driver_id", "name"],
        value_ranges={"experience_years": (0.0, 60.0)},
    ),
    "vehicles": TableContract(
        schema={
            "vehicle_id": pl.Int32(),
            "plate_number": pl.String(),
            "capacity_ton": pl.Float64(),
            "fuel_type": pl.String(),
        },
        primary_keys=["vehicle_id"],
        not_null_columns=["vehicle_id", "plate_number"],
        enums={"fuel_type": APPROVED_FUEL_TYPES},
    ),
    "oil_stations": TableContract(
        schema={
            "station_id": pl.Int32(),
            "station_name": pl.String(),
            "latitude": pl.Float64(),
            "longitude": pl.Float64(),
            "oil_flow_per_day": pl.Float64(),
        },
        primary_keys=["station_id"],
        not_null_columns=["station_id", "station_name", "latitude", "longitude"],
        value_ranges={
            "latitude": (-90.0, 90.0),
            "longitude": (-180.0, 180.0),
            "oil_flow_per_day": (0.0, None),
        },
    ),
}
