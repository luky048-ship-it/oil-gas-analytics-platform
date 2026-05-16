import polars as pl
from datetime import datetime

def build_mart_well_kpi(
    lf_mart_production: pl.LazyFrame,
    lf_gold_history: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Builds mart_well_kpi LazyFrame.
    - Concatenates current production batch with historical gold data for window calculations.
    - Calculates rolling averages and cumulative totals.
    - Ranks wells by production.
    - Assigns performance groups.
    """

    # Combine history and new batch to ensure correct windowing
    # We only need relevant columns for KPI
    combined = pl.concat([
        lf_gold_history.select(lf_mart_production.columns),
        lf_mart_production
    ]).unique(subset=["well_id", "date"], keep="last")

    # 1. Rolling and Cumulative metrics
    kpi_lf = (
        combined
        .sort(["well_id", "date"])
        .with_columns([
            pl.col("oil_ton").rolling_mean(window_size=7).over("well_id").alias("avg_daily_oil"),
            pl.col("oil_ton").cum_sum().over("well_id").alias("total_oil"),
            pl.col("downtime_pct").rolling_mean(window_size=7).over("well_id").alias("avg_downtime_pct"),
            pl.col("production_efficiency").rolling_mean(window_size=7).over("well_id").alias("avg_efficiency"),
            pl.col("oil_ton").max().over("well_id").alias("best_day_oil"),
            pl.col("oil_ton").min().over("well_id").alias("worst_day_oil")
        ])
    )

    # 2. Ranking and Performance Groups (calculated per day)
    kpi_lf = (
        kpi_lf
        .with_columns(
            pl.col("oil_ton").rank(descending=True).over("date").alias("production_rank")
        )
        .with_columns(
            pl.when(pl.col("production_rank") <= 10).then(pl.lit("Top"))
            .when(pl.col("production_rank") <= 30).then(pl.lit("Good"))
            .when(pl.col("production_rank") <= 70).then(pl.lit("Average"))
            .otherwise(pl.lit("Poor"))
            .alias("performance_group")
        )
    )

    kpi_lf = kpi_lf.with_columns([
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.col("date").alias("partition_date")
    ])

    # Return only the rows that were in the original new batch (based on date)
    # Actually, we should return the entire set of dates present in lf_mart_production
    target_dates = lf_mart_production.select("date").unique()

    return (
        kpi_lf
        .join(target_dates, on="date", how="inner")
        .select([
            "well_id", "date", "avg_daily_oil", "total_oil", "avg_downtime_pct",
            "avg_efficiency", "best_day_oil", "worst_day_oil", "production_rank",
            "performance_group", "load_timestamp", "partition_date"
        ])
    )
