# gold_layer/config.py
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import polars as pl

# ---------------------------------------------------------------------------
# Глобальные параметры
# ---------------------------------------------------------------------------
ANALYSIS_PARAMS = {
    "z_score_threshold": 3.0,
    "risk_rolling_window": 7 * 24 * 60,  # 7 дней в минутах (для окон)
    "kpi_rolling_window": 7,  # 7 дней для mart_well_kpi
}


# ---------------------------------------------------------------------------
# Вспомогательные структуры
# ---------------------------------------------------------------------------
@dataclass
class ColumnMapping:
    """Прямое или агрегированное отображение колонки источника на витрину."""

    target: str  # имя колонки в витрине
    source_table: str  # silver-таблица
    source_col: str  # исходная колонка
    agg_func: Optional[str] = None  # "sum", "mean", "max", "min", "first"
    default: Any = None  # значение при отсутствии данных


@dataclass
class WindowAggregation:
    """Оконная агрегация (rolling) с использованием expression Polars."""

    target: str
    source_table: str
    source_col: str
    agg_func: str  # "mean", "sum", "max", "min"
    partition_by: List[str]  # например, ["well_id"]
    order_by: str  # колонка времени
    window_expr: str  # ссылка на глобальный параметр


@dataclass
class JoinSpec:
    """Описание джойна между silver-таблицами."""

    right_table: str
    left_on: List[str]
    right_on: List[str]
    how: str = "left"


@dataclass
class DerivedColumn:
    """Вычисляемая колонка на основе expression."""

    target: str
    expression: str  # например, "pl.col('oil_ton') / pl.col('target_ton')"
    condition: Optional[str] = None  # условие применения


@dataclass
class BusinessRule:
    """Проверка качества данных в витрине."""

    rule: str
    severity: str = "HIGH"  # CRITICAL, HIGH, MEDIUM, LOW


@dataclass
class MartSpec:
    """Полная спецификация построения одной витрины."""

    table_name: str
    # Источники данных
    source_tables: List[str]
    # Агрегации обычные (group_by)
    group_by: Optional[List[str]] = None
    aggregations: List[ColumnMapping] = field(default_factory=list)
    # Оконные агрегации
    window_aggregations: List[WindowAggregation] = field(default_factory=list)
    # Джойны между silver-таблицами
    joins: List[JoinSpec] = field(default_factory=list)
    # Вычисляемые колонки
    derived_columns: List[DerivedColumn] = field(default_factory=list)
    # Финальный набор колонок и их Polars-типы (соответствует DDL)
    output_schema: Dict[str, Union[pl.DataType, type[pl.DataType]]] = field(
        default_factory=dict
    )
    # Первичный ключ
    primary_key: List[str] = field(default_factory=list)
    # Правила качества
    business_rules: List[BusinessRule] = field(default_factory=list)
    # Партиционирование
    partition_column: Optional[str] = None


# ---------------------------------------------------------------------------
# Конфигурация витрин
# ---------------------------------------------------------------------------
MART_CONTRACTS = {
    "mart_production": MartSpec(
        table_name="gold.mart_production",
        source_tables=["production", "well_telemetry", "well_targets"],
        group_by=["well_id", "date"],
        aggregations=[
            # Из daily production
            ColumnMapping("oil_ton", "production", "oil_ton", "first", 0.0),
            ColumnMapping("gas_m3", "production", "gas_m3", "first", 0.0),
            ColumnMapping("water_m3", "production", "water_m3", "first", 0.0),
            ColumnMapping("energy_kwh", "production", "energy_kwh", "first", 0.0),
            ColumnMapping(
                "downtime_hours", "production", "downtime_hours", "first", 0.0
            ),
            # Из телеметрии (агрегация до дня)
            ColumnMapping("avg_temperature", "well_telemetry", "temperature", "mean"),
            ColumnMapping("avg_pressure", "well_telemetry", "pressure", "mean"),
            ColumnMapping(
                "avg_pump_speed_rpm",
                "well_telemetry",
                "pump_speed_rpm",
                "mean",
            ),
            ColumnMapping(
                "avg_oil_flow_rate", "well_telemetry", "oil_flow_rate", "mean"
            ),
            ColumnMapping("max_vibration", "well_telemetry", "vibration", "max"),
            ColumnMapping("daily_oil_ton", "well_targets", "daily_oil_ton", "first"),
        ],
        joins=[
            JoinSpec(
                right_table="well_targets",
                left_on=["well_id", "date"],
                right_on=["well_id", "date"],
                how="left",
            )
        ],
        derived_columns=[
            DerivedColumn("daily_target_ton", "pl.col('daily_oil_ton')"),
            DerivedColumn(
                "production_efficiency",
                "pl.when(pl.col('daily_target_ton') > 0)"
                ".then(pl.col('oil_ton') / pl.col('daily_target_ton'))"
                ".otherwise(0.0)",
            ),
            DerivedColumn(
                "downtime_pct",
                "pl.min_horizontal(" "pl.col('downtime_hours') / 24 * 100, 100.0" ")",
            ),
        ],
        output_schema={
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
        primary_key=["well_id", "date"],
        business_rules=[
            BusinessRule("oil_ton >= 0", "HIGH"),
            BusinessRule("production_efficiency between 0 and 10", "MEDIUM"),
            BusinessRule("downtime_pct between 0 and 100", "HIGH"),
        ],
        partition_column="date",
    ),
    "mart_well_kpi": MartSpec(
        table_name="gold.mart_well_kpi",
        source_tables=["mart_production_batch", "mart_production_history"],
        group_by=["well_id", "date"],
        aggregations=[
            ColumnMapping("oil_ton", "production", "oil_ton", "first", 0.0),
            ColumnMapping(
                "downtime_hours", "production", "downtime_hours", "first", 0.0
            ),
            ColumnMapping("daily_target_ton", "well_targets", "daily_oil_ton", "first"),
        ],
        window_aggregations=[
            WindowAggregation(
                "avg_daily_oil",
                "production",
                "oil_ton",
                agg_func="mean",
                partition_by=["well_id"],
                order_by="date",
                window_expr="kpi_rolling_window",
            ),
            WindowAggregation(
                "total_oil",
                "production",
                "oil_ton",
                agg_func="sum",
                partition_by=["well_id"],
                order_by="date",
                window_expr="kpi_rolling_window",
            ),
            WindowAggregation(
                "best_day_oil",
                "production",
                "oil_ton",
                agg_func="max",
                partition_by=["well_id"],
                order_by="date",
                window_expr="kpi_rolling_window",
            ),
            WindowAggregation(
                "worst_day_oil",
                "production",
                "oil_ton",
                agg_func="min",
                partition_by=["well_id"],
                order_by="date",
                window_expr="kpi_rolling_window",
            ),
            WindowAggregation(
                "avg_downtime_pct",
                "production",
                "downtime_hours",
                agg_func="mean",
                partition_by=["well_id"],
                order_by="date",
                window_expr="kpi_rolling_window",
            ),
        ],
        derived_columns=[
            DerivedColumn(
                "avg_efficiency",
                "pl.when(pl.col('daily_target_ton') > 0)"
                ".then(pl.col('oil_ton') / pl.col('daily_target_ton'))"
                ".otherwise(0.0)",
            ),
            DerivedColumn(
                "production_rank",
                "pl.col('oil_ton').rank('dense', descending=True)" ".over('date')",
            ),
            DerivedColumn(
                "performance_group",
                "pl.when("
                "pl.col('production_rank') <= "
                "pl.col('production_rank').max().over('date') * 0.25"
                ")"
                ".then(pl.lit('Top'))"
                ".when("
                "pl.col('production_rank') <= "
                "pl.col('production_rank').max().over('date') * 0.5"
                ")"
                ".then(pl.lit('Good'))"
                ".when("
                "pl.col('production_rank') <= "
                "pl.col('production_rank').max().over('date') * 0.75"
                ")"
                ".then(pl.lit('Average'))"
                ".otherwise(pl.lit('Poor'))",
            ),
        ],
        output_schema={
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
        primary_key=["well_id", "date"],
        business_rules=[
            BusinessRule("avg_downtime_pct between 0 and 100", "HIGH"),
        ],
        partition_column="date",
    ),
    "mart_failures": MartSpec(
        table_name="gold.mart_failures",
        source_tables=["pump_sensors", "pump_failures", "pumps"],
        joins=[
            JoinSpec(
                right_table="pump_failures",
                left_on=["pump_id", "timestamp"],
                right_on=["pump_id", "failure_date"],
                how="left",
            ),
            JoinSpec(
                right_table="pumps",
                left_on=["pump_id"],
                right_on=["pump_id"],
                how="left",
            ),
        ],
        window_aggregations=[
            WindowAggregation(
                "vibration_zscore",
                "pump_sensors",
                "vibration",
                agg_func="zscore",
                partition_by=["pump_id"],
                order_by="timestamp",
                window_expr="risk_rolling_window",
            ),
            WindowAggregation(
                "temperature_zscore",
                "pump_sensors",
                "temperature",
                agg_func="zscore",
                partition_by=["pump_id"],
                order_by="timestamp",
                window_expr="risk_rolling_window",
            ),
        ],
        derived_columns=[
            DerivedColumn(
                "is_anomaly",
                "(pl.col('vibration_zscore').abs() > "
                "ANALYSIS_PARAMS['z_score_threshold']) | "
                "(pl.col('temperature_zscore').abs() > "
                "ANALYSIS_PARAMS['z_score_threshold'])",
            ),
            DerivedColumn(
                "anomaly_reason",
                "pl.concat_list(["
                "pl.when(pl.col('vibration_zscore').abs() > "
                "ANALYSIS_PARAMS['z_score_threshold'])"
                ".then(pl.lit('vibration')).otherwise(pl.lit(None)), "
                "pl.when(pl.col('temperature_zscore').abs() > "
                "ANALYSIS_PARAMS['z_score_threshold'])"
                ".then(pl.lit('temperature')).otherwise(pl.lit(None))"
                "]).list.drop_nulls()",
            ),
            DerivedColumn("is_failure", "pl.col('failure_type').is_not_null()"),
            DerivedColumn(
                "risk_score",
                "(pl.col('vibration_zscore').abs().clip(0, 4) + "
                "pl.col('temperature_zscore').abs().clip(0, 4)) / 8",
            ),
            DerivedColumn("failure_probability", "pl.col('risk_score')"),
        ],
        output_schema={
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
        business_rules=[
            BusinessRule("risk_score between 0 and 1", "HIGH"),
            BusinessRule("failure_probability between 0 and 1", "HIGH"),
        ],
        partition_column="date",
    ),
    "mart_logistics": MartSpec(
        table_name="gold.mart_logistics",
        source_tables=["deliveries", "drivers", "vehicles"],
        joins=[
            JoinSpec(
                right_table="drivers",
                left_on=["driver_id"],
                right_on=["driver_id"],
                how="left",
            ),
            JoinSpec(
                right_table="vehicles",
                left_on=["vehicle_id"],
                right_on=["vehicle_id"],
                how="left",
            ),
        ],
        derived_columns=[
            DerivedColumn(
                "cost_per_km",
                "pl.when(pl.col('distance_km') > 0)"
                ".then(pl.col('cost_usd') / pl.col('distance_km'))"
                ".otherwise(0.0)",
            ),
            DerivedColumn(
                "cost_per_ton",
                "pl.when(pl.col('volume_ton') > 0)"
                ".then(pl.col('cost_usd') / pl.col('volume_ton'))"
                ".otherwise(0.0)",
            ),
            DerivedColumn("delay_flag", "pl.col('delay_hours') > 0"),
            DerivedColumn(
                "weather_impact",
                "pl.when(pl.col('weather_conditions').str.contains("
                "'storm|rain|snow'"
                ")).then(pl.lit('high'))"
                ".when(pl.col('weather_conditions').str.contains("
                "'cloud|wind'"
                ")).then(pl.lit('medium'))"
                ".otherwise(pl.lit('low'))",
            ),
        ],
        output_schema={
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
        business_rules=[
            BusinessRule("volume_ton >= 0", "HIGH"),
            BusinessRule("cost_per_km >= 0", "MEDIUM"),
        ],
        partition_column="date",
    ),
}
