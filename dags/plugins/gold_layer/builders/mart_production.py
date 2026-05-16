import polars as pl
from datetime import datetime

def build_mart_production(
    lf_production: pl.LazyFrame,
    lf_telemetry: pl.LazyFrame,
    lf_targets: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Builds mart_production LazyFrame.
    - Aggregates telemetry to daily averages/max.
    - Joins with production data.
    - Joins with daily targets.
    - Calculates efficiency and downtime percentage.
    """

    # 1. Aggregate Telemetry to Daily Grain
    telemetry_daily = (
        lf_telemetry
        .with_columns(
            pl.col("timestamp").dt.date().alias("date")
        )
        .group_by(["well_id", "date"])
        .agg([
            pl.col("temperature").mean().alias("avg_temperature"),
            pl.col("pressure").mean().alias("avg_pressure"),
            pl.col("pump_speed_rpm").mean().alias("avg_pump_speed_rpm"),
            pl.col("oil_flow_rate").mean().alias("avg_oil_flow_rate"),
            pl.col("vibration").max().alias("max_vibration")
        ])
    )

    # 2. Join Production with Aggregated Telemetry
    production_enriched = (
        lf_production
        .join(telemetry_daily, on=["well_id", "date"], how="left")
    )

    # 3. Join with Targets
    mart_lf = (
        production_enriched
        .join(lf_targets, on=["well_id", "date"], how="left")
        .rename({"target_ton": "daily_target_ton"})
    )

    # 4. Calculate KPIs
    mart_lf = mart_lf.with_columns([
        (pl.col("oil_ton") / pl.col("daily_target_ton")).alias("production_efficiency"),
        (pl.col("downtime_hours") / 24.0 * 100.0).alias("downtime_pct"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.col("date").alias("partition_date")
    ])

    return mart_lf.select([
        "well_id", "date", "oil_ton", "gas_m3", "water_m3", "energy_kwh",
        "downtime_hours", "avg_temperature", "avg_pressure", "avg_pump_speed_rpm",
        "avg_oil_flow_rate", "max_vibration", "daily_target_ton",
        "production_efficiency", "downtime_pct", "load_timestamp", "partition_date"
    ])
