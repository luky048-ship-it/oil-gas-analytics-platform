"""
Единый центр правды – Schema Contracts для Oil Production Analytics Platform.

Версия: 2.0 — Выверено с реальным DDL (init-sql/*.sql)
Все определения обязательны для Data Producers, ETL/ELT, Streaming, DQ, ML и BI.

КОНВЕНЦИИ:
- Все числовые метрики в Polars: Float64 (точность DECIMAL/NUMERIC на стороне PostgreSQL DDL)
- Все временные метки: Datetime("us") UTC
- Имена: snake_case, pluralized entities
- ID поля: Int32 (суррогатные ключи)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import polars as pl

# =============================================================================
# 1. ПЕРЕЧИСЛЕНИЯ И КОНСТАНТЫ
# =============================================================================


class Severity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class SecurityClassification(Enum):
    CONFIDENTIAL = "confidential"
    INTERNAL = "internal"
    RESTRICTED = "restricted"


class UpdatePattern(Enum):
    UPSERT = "UPSERT"
    INSERT_ONLY = "INSERT_ONLY"


# Утверждённые значения enum-полей (выверены с DDL и бизнес-семантикой)
APPROVED_WELL_STATUS = ["active", "suspended", "maintenance", "decommissioned"]
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
APPROVED_ETL_STATUS = ["processing", "loaded", "failed", "skipped"]
APPROVED_PERFORMANCE_GROUPS = ["Top", "Good", "Average", "Poor"]


# =============================================================================
# 2. СТРУКТУРЫ КОНТРАКТОВ
# =============================================================================


@dataclass
class ForeignKey:
    """Связь внешнего ключа."""

    column: str
    parent_table: str
    parent_column: str


@dataclass
class ValidationRule:
    """Правило валидации данных."""

    rule_type: str  # "range", "enum", "custom", "not_null"
    severity: Severity
    params: Dict = field(default_factory=dict)


@dataclass
class StreamingSpec:
    """Спецификация стриминга."""

    late_arrival_window_minutes: int
    dedup_key: List[str]
    ordering_key: str
    expected_frequency_seconds: Tuple[int, int]


@dataclass
class TableConfig:
    """Полный контракт одной таблицы — истина для всех слоёв."""

    table_name: str
    layer: str  # raw, bronze, silver, gold, metadata
    security: SecurityClassification
    schema: Dict[str, pl.DataType]
    primary_key: List[str]
    not_null_columns: List[str]
    foreign_keys: List[ForeignKey] = field(default_factory=list)
    unique_columns: List[str] = field(default_factory=list)
    validation_rules: List[ValidationRule] = field(default_factory=list)
    partition_column: Optional[str] = None
    clustering_columns: List[str] = field(default_factory=list)
    freshness_sla_hours: Optional[float] = None
    streaming: Optional[StreamingSpec] = None
    update_pattern: UpdatePattern = UpdatePattern.UPSERT
    retention_days: Optional[int] = None
    comment: str = ""


# =============================================================================
# 3. РЕЕСТР ВСЕХ ТАБЛИЦ (Single Source of Truth)
# =============================================================================

TABLE_CONTRACTS: Dict[str, TableConfig] = {
    # =========================================================================
    # MASTER DATA
    # =========================================================================
    "wells": TableConfig(
        table_name="wells",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "well_id": pl.Int32,
            "name": pl.Utf8,
            "field_name": pl.Utf8,
            "region": pl.Utf8,
            "start_date": pl.Date,
            "operator": pl.Utf8,
            "status": pl.Utf8,
        },
        primary_key=["well_id"],
        not_null_columns=["well_id", "name"],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "status", "values": APPROVED_WELL_STATUS},
            ),
            ValidationRule(
                "custom", Severity.HIGH, {"expression": "start_date <= CURRENT_DATE"}
            ),
        ],
        freshness_sla_hours=24,
        comment="Master data for oil wells. DDL: wells (well_id SERIAL PK)",
    ),
    # =========================================================================
    # PRODUCTION & TELEMETRY
    # =========================================================================
    "production": TableConfig(
        table_name="production",
        layer="silver",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "prod_id": pl.Int32,
            "well_id": pl.Int32,
            "date": pl.Date,
            "oil_ton": pl.Float64,
            "gas_m3": pl.Float64,
            "water_m3": pl.Float64,
            "energy_kwh": pl.Float64,
            "downtime_hours": pl.Float64,
            "temperature": pl.Float64,
            "pressure": pl.Float64,
        },
        primary_key=["prod_id"],
        not_null_columns=["prod_id", "well_id", "date"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        validation_rules=[
            ValidationRule("range", Severity.HIGH, {"column": "oil_ton", "min": 0.0}),
            ValidationRule("range", Severity.HIGH, {"column": "gas_m3", "min": 0.0}),
            ValidationRule("range", Severity.HIGH, {"column": "water_m3", "min": 0.0}),
            ValidationRule(
                "range", Severity.HIGH, {"column": "energy_kwh", "min": 0.0}
            ),
            ValidationRule(
                "range",
                Severity.HIGH,
                {"column": "downtime_hours", "min": 0.0, "max": 24.0},
            ),
            ValidationRule(
                "range",
                Severity.MEDIUM,
                {"column": "pressure", "min": 0.0, "max": 1000.0},
            ),
            ValidationRule(
                "range",
                Severity.MEDIUM,
                {"column": "temperature", "min": -60.0, "max": 250.0},
            ),
        ],
        partition_column="date",
        clustering_columns=["well_id"],
        freshness_sla_hours=24,
        comment="Daily production metrics per well. DDL: production (prod_id SERIAL PK, FK wells)",
    ),
    "well_telemetry": TableConfig(
        table_name="well_telemetry",
        layer="bronze",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "record_id": pl.Int32,
            "well_id": pl.Int32,
            "timestamp": pl.Datetime("us"),
            "pump_speed_rpm": pl.Float64,
            "pump_current": pl.Float64,
            "pressure_in": pl.Float64,
            "pressure_out": pl.Float64,
            "temperature": pl.Float64,
            "vibration": pl.Float64,
            "oil_flow_rate": pl.Float64,
        },
        primary_key=["record_id"],
        not_null_columns=["record_id"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        validation_rules=[
            ValidationRule(
                "range", Severity.HIGH, {"column": "pump_speed_rpm", "min": 0.0}
            ),
            ValidationRule("range", Severity.HIGH, {"column": "vibration", "min": 0.0}),
            ValidationRule(
                "range", Severity.HIGH, {"column": "oil_flow_rate", "min": 0.0}
            ),
            ValidationRule(
                "custom", Severity.MEDIUM, {"expression": "pressure_out >= pressure_in"}
            ),
        ],
        partition_column="event_date",
        clustering_columns=["well_id"],
        freshness_sla_hours=10 / 60,
        streaming=StreamingSpec(
            late_arrival_window_minutes=10,
            dedup_key=["record_id"],
            ordering_key="timestamp",
            expected_frequency_seconds=(1, 10),
        ),
        comment="High-frequency telemetry from wells. DDL: well_telemetry (record_id SERIAL PK, FK wells)",
    ),
    "well_targets": TableConfig(
        table_name="well_targets",
        layer="silver",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "well_id": pl.Int32,
            "date": pl.Date,
            "daily_oil_ton": pl.Float64,
        },
        primary_key=[],
        not_null_columns=["daily_oil_ton"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        unique_columns=["well_id", "date"],
        validation_rules=[
            ValidationRule(
                "range", Severity.HIGH, {"column": "daily_oil_ton", "min": 0.0}
            ),
        ],
        comment="Daily oil production targets. DDL: well_targets (нет PK, unique(well_id, date))",
    ),
    # =========================================================================
    # PUMP & SENSOR
    # =========================================================================
    "pumps": TableConfig(
        table_name="pumps",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "pump_id": pl.Int32,
            "well_id": pl.Int32,
            "type": pl.Utf8,
            "install_date": pl.Date,
            "manufacturer": pl.Utf8,
            "model": pl.Utf8,
        },
        primary_key=["pump_id"],
        not_null_columns=["pump_id"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        validation_rules=[
            ValidationRule(
                "custom", Severity.HIGH, {"expression": "install_date <= CURRENT_DATE"}
            ),
        ],
        comment="Pump asset registry. DDL: pumps (pump_id SERIAL PK, FK wells)",
    ),
    "pump_sensors": TableConfig(
        table_name="pump_sensors",
        layer="bronze",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "record_id": pl.Int32,
            "pump_id": pl.Int32,
            "timestamp": pl.Datetime("us"),
            "temperature": pl.Float64,
            "vibration": pl.Float64,
            "current": pl.Float64,
            "rpm": pl.Float64,
            "pressure": pl.Float64,
        },
        primary_key=["record_id"],
        not_null_columns=["record_id"],
        foreign_keys=[ForeignKey("pump_id", "pumps", "pump_id")],
        validation_rules=[
            ValidationRule("range", Severity.HIGH, {"column": "vibration", "min": 0.0}),
            ValidationRule("range", Severity.HIGH, {"column": "rpm", "min": 0.0}),
            ValidationRule("range", Severity.HIGH, {"column": "pressure", "min": 0.0}),
        ],
        partition_column="event_date",
        clustering_columns=["pump_id"],
        freshness_sla_hours=5 / 60,
        streaming=StreamingSpec(
            late_arrival_window_minutes=5,
            dedup_key=["record_id"],
            ordering_key="timestamp",
            expected_frequency_seconds=(1, 5),
        ),
        comment="Telemetry from pump sensor systems. DDL: pump_sensors (record_id SERIAL PK, FK pumps)",
    ),
    "pump_failures": TableConfig(
        table_name="pump_failures",
        layer="silver",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "failure_id": pl.Int32,
            "pump_id": pl.Int32,
            "failure_date": pl.Datetime("us"),
            "failure_type": pl.Utf8,
            "downtime_hours": pl.Float64,
        },
        primary_key=["failure_id"],
        not_null_columns=["failure_id"],
        foreign_keys=[ForeignKey("pump_id", "pumps", "pump_id")],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "failure_type", "values": APPROVED_FAILURE_TYPES},
            ),
            ValidationRule(
                "range", Severity.HIGH, {"column": "downtime_hours", "min": 0.0}
            ),
            ValidationRule("not_null", Severity.HIGH, {"column": "failure_type"}),
        ],
        partition_column="failure_month",
        clustering_columns=["pump_id"],
        comment="Pump failure events. DDL: pump_failures (failure_id SERIAL PK, FK pumps)",
    ),
    # =========================================================================
    # LOGISTICS
    # =========================================================================
    "deliveries": TableConfig(
        table_name="deliveries",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "delivery_id": pl.Int32,
            "date": pl.Date,
            "source": pl.Utf8,
            "destination": pl.Utf8,
            "product_type": pl.Utf8,
            "volume_ton": pl.Float64,
            "cost_usd": pl.Float64,
            "delay_hours": pl.Float64,
            "distance_km": pl.Float64,
            "weather_conditions": pl.Utf8,
            "driver_id": pl.Int32,
            "vehicle_id": pl.Int32,
        },
        primary_key=["delivery_id"],
        not_null_columns=["delivery_id"],
        foreign_keys=[
            ForeignKey("driver_id", "drivers", "driver_id"),
            ForeignKey("vehicle_id", "vehicles", "vehicle_id"),
        ],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "product_type", "values": APPROVED_PRODUCT_TYPES},
            ),
            ValidationRule(
                "range", Severity.HIGH, {"column": "volume_ton", "min": 0.0}
            ),
            ValidationRule("range", Severity.HIGH, {"column": "cost_usd", "min": 0.0}),
            ValidationRule(
                "range", Severity.MEDIUM, {"column": "delay_hours", "min": 0.0}
            ),
            ValidationRule(
                "range", Severity.HIGH, {"column": "distance_km", "min": 0.0001}
            ),
        ],
        partition_column="date",
        clustering_columns=["source"],
        comment="Oil logistics and transportation events. DDL: deliveries (delivery_id SERIAL PK, FK driver_id, vehicle_id)",
    ),
    "drivers": TableConfig(
        table_name="drivers",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "driver_id": pl.Int32,
            "name": pl.Utf8,
            "experience_years": pl.Int32,
            "region": pl.Utf8,
        },
        primary_key=["driver_id"],
        not_null_columns=["driver_id"],
        validation_rules=[
            ValidationRule(
                "range",
                Severity.MEDIUM,
                {"column": "experience_years", "min": 0, "max": 60},
            ),
        ],
        comment="Driver master registry. DDL: drivers (driver_id SERIAL PK)",
    ),
    "vehicles": TableConfig(
        table_name="vehicles",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "vehicle_id": pl.Int32,
            "plate_number": pl.Utf8,
            "capacity_ton": pl.Float64,
            "fuel_type": pl.Utf8,
        },
        primary_key=["vehicle_id"],
        not_null_columns=["vehicle_id"],
        unique_columns=["plate_number"],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "fuel_type", "values": APPROVED_FUEL_TYPES},
            ),
            ValidationRule(
                "range", Severity.HIGH, {"column": "capacity_ton", "min": 0.0001}
            ),
        ],
        comment="Fleet registry. DDL: vehicles (vehicle_id SERIAL PK)",
    ),
    "oil_stations": TableConfig(
        table_name="oil_stations",
        layer="silver",
        security=SecurityClassification.INTERNAL,
        schema={
            "station_id": pl.Int32,
            "station_name": pl.Utf8,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "oil_flow_per_day": pl.Float64,
        },
        primary_key=["station_id"],
        not_null_columns=["station_id"],
        validation_rules=[
            ValidationRule(
                "range",
                Severity.HIGH,
                {"column": "latitude", "min": -90.0, "max": 90.0},
            ),
            ValidationRule(
                "range",
                Severity.HIGH,
                {"column": "longitude", "min": -180.0, "max": 180.0},
            ),
            ValidationRule(
                "range", Severity.HIGH, {"column": "oil_flow_per_day", "min": 0.0}
            ),
        ],
        comment="Oil station operational registry. DDL: oil_stations (station_id SERIAL PK)",
    ),
    # =========================================================================
    # ETL METADATA
    # =========================================================================
    "etl_metadata.loaded_partitions": TableConfig(
        table_name="etl_metadata.loaded_partitions",
        layer="metadata",
        security=SecurityClassification.INTERNAL,
        schema={
            "table_name": pl.Utf8,
            "partition_date": pl.Date,
            "status": pl.Utf8,
            "loaded_at": pl.Datetime("us"),
            "updated_at": pl.Datetime("us"),
            "dag_run_id": pl.Utf8,
        },
        primary_key=[],
        not_null_columns=["table_name", "partition_date", "status"],
        unique_columns=["table_name", "partition_date"],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "status", "values": APPROVED_ETL_STATUS},
            ),
            ValidationRule(
                "custom",
                Severity.HIGH,
                {"expression": "partition_date <= CURRENT_DATE + 1"},
            ),
        ],
        partition_column="partition_date",
        clustering_columns=["table_name"],
        update_pattern=UpdatePattern.UPSERT,
        retention_days=None,
        comment="Incremental ingestion orchestration metadata. DDL: etl_metadata.loaded_partitions",
    ),
    "etl_metadata.marts_loaded_partitions": TableConfig(
        table_name="etl_metadata.marts_loaded_partitions",
        layer="metadata",
        security=SecurityClassification.INTERNAL,
        schema={
            "mart_name": pl.Utf8,
            "partition_date": pl.Date,
            "dag_run_id": pl.Utf8,
            "loaded_at": pl.Datetime("us"),
        },
        primary_key=[],
        not_null_columns=["mart_name", "partition_date"],
        unique_columns=["mart_name", "partition_date"],
        validation_rules=[
            ValidationRule(
                "custom",
                Severity.HIGH,
                {"expression": "partition_date <= CURRENT_DATE + 1"},
            ),
        ],
        partition_column="partition_date",
        clustering_columns=["mart_name"],
        update_pattern=UpdatePattern.INSERT_ONLY,
        retention_days=None,
        comment="Gold/DataMart incremental load tracking. DDL: etl_metadata.marts_loaded_partitions",
    ),
    # =========================================================================
    # GOLD MARTS
    # =========================================================================
    "gold.mart_production": TableConfig(
        table_name="gold.mart_production",
        layer="gold",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "mart_id": pl.Int64,
            "well_id": pl.Int32,
            "date": pl.Date,
            "oil_ton": pl.Float64,
            "gas_m3": pl.Float64,
            "water_m3": pl.Float64,
            "energy_kwh": pl.Float64,
            "downtime_hours": pl.Float64,
            "avg_temperature": pl.Float64,
            "avg_pressure": pl.Float64,
            "avg_pump_speed_rpm": pl.Float64,
            "avg_oil_flow_rate": pl.Float64,
            "max_vibration": pl.Float64,
            "daily_target_ton": pl.Float64,
            "production_efficiency": pl.Float64,
            "downtime_pct": pl.Float64,
            "load_timestamp": pl.Datetime("us"),
            "partition_date": pl.Date,
        },
        primary_key=["mart_id"],
        not_null_columns=["mart_id", "well_id", "date", "partition_date"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        validation_rules=[
            ValidationRule(
                "range",
                Severity.MEDIUM,
                {"column": "production_efficiency", "min": 0.0, "max": 10.0},
            ),
            ValidationRule(
                "range",
                Severity.HIGH,
                {"column": "downtime_pct", "min": 0.0, "max": 100.0},
            ),
            ValidationRule("range", Severity.HIGH, {"column": "oil_ton", "min": 0.0}),
        ],
        partition_column="partition_date",
        clustering_columns=["well_id"],
        comment="Production mart aggregated by well and operational day. DDL: gold.mart_production",
    ),
    "gold.mart_well_kpi": TableConfig(
        table_name="gold.mart_well_kpi",
        layer="gold",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "well_id": pl.Int32,
            "date": pl.Date,
            "avg_daily_oil": pl.Float64,
            "total_oil": pl.Float64,
            "avg_downtime_pct": pl.Float64,
            "avg_efficiency": pl.Float64,
            "best_day_oil": pl.Float64,
            "worst_day_oil": pl.Float64,
            "production_rank": pl.Int32,
            "performance_group": pl.Utf8,
            "load_timestamp": pl.Datetime("us"),
            "partition_date": pl.Date,
        },
        primary_key=[],
        not_null_columns=["well_id", "date", "partition_date"],
        unique_columns=["well_id", "date"],
        foreign_keys=[ForeignKey("well_id", "wells", "well_id")],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "performance_group", "values": APPROVED_PERFORMANCE_GROUPS},
            ),
        ],
        partition_column="partition_date",
        clustering_columns=["performance_group"],
        comment="Analytical KPI mart for well performance benchmarking. DDL: gold.mart_well_kpi",
    ),
    "gold.mart_failures": TableConfig(
        table_name="gold.mart_failures",
        layer="gold",
        security=SecurityClassification.CONFIDENTIAL,
        schema={
            "record_id": pl.Int64,
            "pump_id": pl.Int32,
            "well_id": pl.Int32,
            "date": pl.Date,
            "timestamp": pl.Datetime("us"),
            "temperature": pl.Float64,
            "vibration": pl.Float64,
            "current": pl.Float64,
            "rpm": pl.Float64,
            "pressure": pl.Float64,
            "vibration_zscore": pl.Float64,
            "temperature_zscore": pl.Float64,
            "is_anomaly": pl.Boolean,
            "anomaly_reason": pl.List(pl.Utf8),
            "failure_type": pl.Utf8,
            "is_failure": pl.Boolean,
            "risk_score": pl.Float64,
            "failure_probability": pl.Float64,
            "load_timestamp": pl.Datetime("us"),
            "partition_date": pl.Date,
        },
        primary_key=["record_id"],
        not_null_columns=["record_id", "pump_id", "well_id", "date", "partition_date"],
        foreign_keys=[
            ForeignKey("pump_id", "pumps", "pump_id"),
            ForeignKey("well_id", "wells", "well_id"),
        ],
        validation_rules=[
            ValidationRule(
                "range", Severity.HIGH, {"column": "risk_score", "min": 0.0, "max": 1.0}
            ),
            ValidationRule(
                "range",
                Severity.HIGH,
                {"column": "failure_probability", "min": 0.0, "max": 1.0},
            ),
        ],
        partition_column="partition_date",
        clustering_columns=["pump_id"],
        comment="Predictive maintenance and anomaly detection mart. DDL: gold.mart_failures",
    ),
    "gold.mart_logistics": TableConfig(
        table_name="gold.mart_logistics",
        layer="gold",
        security=SecurityClassification.INTERNAL,
        schema={
            "delivery_id": pl.Int64,
            "date": pl.Date,
            "source": pl.Utf8,
            "destination": pl.Utf8,
            "product_type": pl.Utf8,
            "volume_ton": pl.Float64,
            "cost_usd": pl.Float64,
            "delay_hours": pl.Float64,
            "distance_km": pl.Float64,
            "weather_conditions": pl.Utf8,
            "driver_id": pl.Int32,
            "driver_name": pl.Utf8,
            "experience_years": pl.Int32,
            "vehicle_id": pl.Int32,
            "plate_number": pl.Utf8,
            "capacity_ton": pl.Float64,
            "fuel_type": pl.Utf8,
            "cost_per_km": pl.Float64,
            "cost_per_ton": pl.Float64,
            "delay_flag": pl.Boolean,
            "weather_impact": pl.Utf8,
            "load_timestamp": pl.Datetime("us"),
            "partition_date": pl.Date,
        },
        primary_key=["delivery_id"],
        not_null_columns=["delivery_id", "date", "partition_date"],
        foreign_keys=[
            ForeignKey("driver_id", "drivers", "driver_id"),
            ForeignKey("vehicle_id", "vehicles", "vehicle_id"),
        ],
        validation_rules=[
            ValidationRule(
                "enum",
                Severity.HIGH,
                {"column": "weather_impact", "values": APPROVED_WEATHER_IMPACT},
            ),
        ],
        partition_column="partition_date",
        clustering_columns=["driver_id"],
        comment="Gold-layer logistics and transportation analytics mart. DDL: gold.mart_logistics",
    ),
}


# =============================================================================
# 4. REFERENTIAL INTEGRITY MATRIX
# =============================================================================


def build_referential_integrity_matrix() -> List[Dict]:
    """Автоматически строит матрицу FK из TABLE_CONTRACTS."""
    matrix = []
    for tbl_conf in TABLE_CONTRACTS.values():
        for fk in tbl_conf.foreign_keys:
            matrix.append(
                {
                    "child_table": tbl_conf.table_name,
                    "fk_column": fk.column,
                    "parent_table": fk.parent_table,
                    "parent_column": fk.parent_column,
                }
            )
    return matrix


REFERENTIAL_INTEGRITY = build_referential_integrity_matrix()


# =============================================================================
# 5. OBSERVABILITY & MONITORING
# =============================================================================


@dataclass
class ObservabilityConfig:
    freshness_sla_breach_minutes: int = 30
    dq_failure_rate_threshold_pct: float = 5.0
    duplicate_rate_threshold_pct: float = 0.1
    null_mandatory_threshold: int = 0
    ingestion_lag_warning_seconds: int = 60


OBSERVABILITY = ObservabilityConfig()
