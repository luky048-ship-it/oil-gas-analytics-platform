import polars as pl
from datetime import datetime

def build_mart_logistics(
    lf_deliveries: pl.LazyFrame,
    lf_drivers: pl.LazyFrame,
    lf_vehicles: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Builds mart_logistics LazyFrame.
    - Joins deliveries with drivers and vehicles.
    - Calculates cost per km and cost per ton.
    - Categorizes weather impact.
    """

    # 1. Joins
    mart_lf = (
        lf_deliveries
        .join(lf_drivers, on="driver_id", how="left")
        .join(lf_vehicles, on="vehicle_id", how="left")
    )

    # 2. KPIs
    mart_lf = mart_lf.with_columns([
        (pl.col("cost_usd") / pl.col("distance_km")).alias("cost_per_km"),
        (pl.col("cost_usd") / pl.col("volume_ton")).alias("cost_per_ton"),
        (pl.col("delay_hours") > 0).alias("delay_flag")
    ])

    # 3. Weather impact
    mart_lf = mart_lf.with_columns(
        pl.when(pl.col("weather_conditions").is_in(["Storm", "Snow", "Heavy Rain"])).then(pl.lit("high"))
        .when(pl.col("weather_conditions").is_in(["Rain", "Fog"])).then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .alias("weather_impact")
    )

    mart_lf = mart_lf.with_columns([
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.col("date").alias("partition_date")
    ])

    return mart_lf.select([
        "delivery_id", "date", "source", "destination", "product_type",
        "volume_ton", "cost_usd", "delay_hours", "distance_km", "weather_conditions",
        "driver_id", "driver_name", "experience_years", "vehicle_id", "plate_number",
        "capacity_ton", "fuel_type", "cost_per_km", "cost_per_ton", "delay_flag",
        "weather_impact", "load_timestamp", "partition_date"
    ])
