from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import polars as pl


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

    # Validation Rules
    foreign_keys: List[ForeignKeyContract] = field(default_factory=list)
    unique_columns: List[str] = field(default_factory=list)
    value_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = field(
        default_factory=dict
    )
    enums: Dict[str, List[str]] = field(default_factory=dict)
    custom_rules: List[str] = field(default_factory=list)

    # Observability & SLAs
    freshness_sla_minutes: Optional[int] = None
    partition_column: Optional[str] = None
    statistical_monitored_columns: List[str] = field(default_factory=list)


# -----------------------------------------------------------------------------
# ENUMS & APPROVED VALUES
# -----------------------------------------------------------------------------

APPROVED_WELL_STATUSES = ["active", "inactive", "maintenance", "decommissioned"]

APPROVED_FAILURE_TYPES = [
    "electrical",
    "mechanical",
    "overheating",
    "seal_failure",
    "vibration_alarm",
    "pressure_loss",
    "unknown",
]

APPROVED_PRODUCT_TYPES = ["crude_oil", "condensate", "diesel", "drilling_fluids"]

APPROVED_FUEL_TYPES = ["diesel", "gasoline", "electric", "hybrid", "lng"]

APPROVED_WEATHER_IMPACT = ["high", "medium", "low"]


# -----------------------------------------------------------------------------
# TABLE CONTRACTS REGISTRY
# -----------------------------------------------------------------------------

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
        not_null_columns=["well_id", "name"],
        enums={"status": APPROVED_WELL_STATUSES},
        custom_rules=["start_date <= current_date"],
        freshness_sla_minutes=1440,
        partition_column=None,
    ),
    "production": TableContract(
        schema={
            "prod_id": pl.Int32(),
            "well_id": pl.Int32(),
            "date": pl.Date(),
            "oil_ton": pl.Decimal(10, 2),  # вместо Float64
            "gas_m3": pl.Decimal(12, 2),
            "water_m3": pl.Decimal(12, 2),
            "energy_kwh": pl.Decimal(12, 2),
            "downtime_hours": pl.Decimal(5, 2),
            "temperature": pl.Decimal(5, 2),
            "pressure": pl.Decimal(5, 2),
        },
        primary_keys=["prod_id"],
        not_null_columns=["prod_id", "date"],
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
        freshness_sla_minutes=1440,  # 24 h
        partition_column="date",
        statistical_monitored_columns=["oil_ton", "gas_m3", "water_m3"],
    ),
    "well_telemetry": TableContract(
        schema={
            "record_id": pl.Int32(),
            "well_id": pl.Int32(),
            "timestamp": pl.Datetime("us"),  # унифицировать с ETL (секунды)
            "pump_speed_rpm": pl.Decimal(8, 2),
            "pump_current": pl.Decimal(8, 2),
            "pressure_in": pl.Decimal(8, 2),
            "pressure_out": pl.Decimal(8, 2),
            "temperature": pl.Decimal(5, 2),
            "vibration": pl.Decimal(5, 2),
            "oil_flow_rate": pl.Decimal(8, 2),
        },
        primary_keys=["record_id"],
        not_null_columns=["record_id"],  # только PK (well_id и timestamp NULL в DDL)
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
        value_ranges={
            "pump_speed_rpm": (0.0, None),
            "vibration": (0.0, None),
            "oil_flow_rate": (0.0, None),
        },
        custom_rules=["pressure_out >= pressure_in"],
        freshness_sla_minutes=10,
        partition_column="event_date",
        statistical_monitored_columns=["vibration", "temperature", "oil_flow_rate"],
    ),
    "well_targets": TableContract(
        schema={
            "well_id": pl.Int32(),
            "date": pl.Date(),
            "daily_oil_ton": pl.Float64(),
        },
        primary_keys=[],
        not_null_columns=[],
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
        not_null_columns=["pump_id"],
        foreign_keys=[ForeignKeyContract("well_id", "wells", "well_id")],
        custom_rules=["install_date <= current_date"],
    ),
    "pump_sensors": TableContract(
        schema={
            "record_id": pl.Int32(),
            "pump_id": pl.Int32(),
            "timestamp": pl.Datetime("us"),
            "temperature": pl.Decimal(5, 2),
            "vibration": pl.Decimal(5, 2),
            "current": pl.Decimal(8, 2),
            "rpm": pl.Decimal(8, 2),
            "pressure": pl.Decimal(8, 2),
        },
        primary_keys=["record_id"],
        not_null_columns=["record_id"],
        foreign_keys=[ForeignKeyContract("pump_id", "pumps", "pump_id")],
        value_ranges={
            "vibration": (0.0, None),
            "rpm": (0.0, None),
            "pressure": (0.0, None),
        },
        freshness_sla_minutes=5,
        partition_column="event_date",
        statistical_monitored_columns=["vibration", "rpm", "temperature"],
    ),
    "pump_failures": TableContract(
        schema={
            "failure_id": pl.Int32(),
            "pump_id": pl.Int32(),
            "failure_date": pl.Datetime("us"),
            "failure_type": pl.String(),
            "downtime_hours": pl.Decimal(5, 2),
        },
        primary_keys=["failure_id"],
        not_null_columns=["failure_id"],
        foreign_keys=[ForeignKeyContract("pump_id", "pumps", "pump_id")],
        value_ranges={"downtime_hours": (0.0, None)},
        enums={"failure_type": APPROVED_FAILURE_TYPES},
        partition_column="failure_month",
    ),
    "deliveries": TableContract(
        schema={
            "delivery_id": pl.Int32(),
            "date": pl.Date(),
            "source": pl.String(),
            "destination": pl.String(),
            "product_type": pl.String(),
            "volume_ton": pl.Decimal(10, 2),
            "cost_usd": pl.Decimal(10, 2),
            "delay_hours": pl.Decimal(6, 2),
            "distance_km": pl.Decimal(8, 2),
            "weather_conditions": pl.String(),
            "driver_id": pl.Int32(),
            "vehicle_id": pl.Int32(),
        },
        primary_keys=["delivery_id"],
        not_null_columns=["delivery_id"],
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
        partition_column="date",
    ),
    "drivers": TableContract(
        schema={
            "driver_id": pl.Int32(),
            "name": pl.String(),
            "experience_years": pl.Int32(),
            "region": pl.String(),
        },
        primary_keys=["driver_id"],
        not_null_columns=["driver_id"],
        value_ranges={"experience_years": (0.0, 60.0)},
    ),
    "vehicles": TableContract(
        schema={
            "vehicle_id": pl.Int32(),
            "plate_number": pl.String(),
            "capacity_ton": pl.Decimal(8, 2),
            "fuel_type": pl.String(),
        },
        primary_keys=["vehicle_id"],
        not_null_columns=["vehicle_id"],
        unique_columns=["plate_number"],
        value_ranges={"capacity_ton": (0.0001, None)},
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
        not_null_columns=["station_id"],
        value_ranges={
            "latitude": (-90.0, 90.0),
            "longitude": (-180.0, 180.0),
            "oil_flow_per_day": (0.0, None),
        },
    ),
}
