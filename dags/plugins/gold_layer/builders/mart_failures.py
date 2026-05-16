import polars as pl
from datetime import datetime
from dags.plugins.gold_layer.config import ANALYSIS_PARAMS

def build_mart_failures(
    lf_sensors: pl.LazyFrame,
    lf_failures: pl.LazyFrame,
    lf_pumps: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Builds mart_failures LazyFrame.
    - Calculates Z-score for vibration and temperature.
    - Identifies anomalies.
    - Joins with historical pump failures.
    - Calculates risk scores.
    """
    threshold = ANALYSIS_PARAMS["z_score_threshold"]
    risk_window = ANALYSIS_PARAMS["risk_rolling_window"]

    # 1. Z-Score Calculation (using window functions over pump_id)
    enriched_sensors = (
        lf_sensors
        .with_columns([
            ((pl.col("vibration") - pl.col("vibration").mean().over("pump_id")) /
             pl.col("vibration").std().over("pump_id")).alias("vibration_zscore"),
            ((pl.col("temperature") - pl.col("temperature").mean().over("pump_id")) /
             pl.col("temperature").std().over("pump_id")).alias("temperature_zscore")
        ])
    )

    # 2. Anomaly Detection
    enriched_sensors = enriched_sensors.with_columns(
        ( (pl.col("vibration_zscore").abs() > threshold) |
          (pl.col("temperature_zscore").abs() > threshold) ).alias("is_anomaly")
    ).with_columns(
        pl.when(pl.col("vibration_zscore").abs() > threshold).then(pl.lit("High Vibration"))
        .when(pl.col("temperature_zscore").abs() > threshold).then(pl.lit("High Temperature"))
        .otherwise(None)
        .alias("anomaly_reason")
    )
    # Convert anomaly_reason to list for TEXT[] compatibility
    enriched_sensors = enriched_sensors.with_columns(
        pl.col("anomaly_reason").map_elements(lambda x: [x] if x else [], return_dtype=pl.List(pl.String)).alias("anomaly_reason")
    )

    # 3. Join with Failures
    enriched_sensors = enriched_sensors.with_columns(
        pl.col("timestamp").dt.date().alias("date")
    )

    daily_failures = (
        lf_failures
        .with_columns(pl.col("failure_date").dt.date().alias("date"))
        .group_by(["pump_id", "date"])
        .agg([
            pl.col("failure_type").first(),
            pl.lit(True).alias("is_failure")
        ])
    )

    mart_lf = (
        enriched_sensors
        .join(daily_failures, on=["pump_id", "date"], how="left")
        .with_columns(pl.col("is_failure").fill_null(False))
    )

    # 4. Risk Score
    mart_lf = (
        mart_lf
        .sort(["pump_id", "timestamp"])
        .with_columns(
            pl.col("is_anomaly").cast(pl.Int32).rolling_mean(window_size=risk_window).over("pump_id").alias("risk_score")
        )
        .fill_null(0)
    )

    # 5. Final joins and metadata
    mart_lf = (
        mart_lf
        .join(lf_pumps.select(["pump_id", "well_id"]), on="pump_id", how="left")
        .with_columns([
            pl.lit(0.0).alias("failure_probability"),
            pl.lit(datetime.now()).alias("load_timestamp"),
            pl.col("date").alias("partition_date")
        ])
    )

    return mart_lf.select([
        "pump_id", "well_id", "date", "timestamp", "temperature", "vibration",
        "current", "rpm", "pressure", "vibration_zscore", "temperature_zscore",
        "is_anomaly", "anomaly_reason", "failure_type", "is_failure",
        "risk_score", "failure_probability", "load_timestamp", "partition_date"
    ])
