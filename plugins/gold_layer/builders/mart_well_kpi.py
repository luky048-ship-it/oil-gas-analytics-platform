import polars as pl


def build_mart_well_kpi(
    lf_prod: pl.LazyFrame, lf_history: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Рассчитывает ключевые показатели эффективности (KPI) скважин (mart_well_kpi).
    Выполняет агрегацию добычи, расчет среднего времени простоя, ранжирование скважин
    по суммарной добыче и классификацию их по группам производительности.
    """

    # 1. Выбор необходимых столбцов для расчета агрегатов
    combined = lf_prod.select(["well_id", "date", "oil_ton", "downtime_hours"])

    # 2. Агрегация производственных показателей по каждой скважине
    kpi = combined.group_by("well_id").agg(
        [
            pl.col("oil_ton").mean().alias("avg_daily_oil"),
            pl.col("oil_ton").sum().alias("total_oil"),
            pl.col("oil_ton").max().alias("best_day_oil"),
            pl.col("oil_ton").min().alias("worst_day_oil"),
            (pl.col("downtime_hours").sum() / (pl.len() * 24)).alias(
                "avg_downtime_pct"
            ),
        ]
    )

    # 3. Ранжирование скважин на основе суммарной добычи
    kpi = kpi.with_columns(
        [
            pl.col("total_oil")
            .rank(descending=True)
            .alias("production_rank")
            .cast(pl.Int32)
        ]
    )

    # 4. Классификация скважин по группам производительности (Performance groups)
    kpi = kpi.with_columns(
        pl.when(pl.col("production_rank") <= 3)
        .then(pl.lit("Top"))
        .when(pl.col("production_rank") <= 10)
        .then(pl.lit("Good"))
        .otherwise(pl.lit("Average"))
        .alias("performance_group")
    )

    # 5. Определение актуальной даты для витрины на основе входных данных
    max_date = lf_prod.select(pl.col("date").max()).collect().item()
    kpi = kpi.with_columns(pl.lit(max_date).alias("date"))

    # 6. Финальное приведение типов данных для соответствия схеме Gold-слоя
    return kpi.select(
        [
            pl.col("well_id").cast(pl.Int32),
            pl.col("date").cast(pl.Date),
            pl.col("avg_daily_oil").cast(pl.Decimal(12, 3)),
            pl.col("total_oil").cast(pl.Decimal(14, 3)),
            pl.col("avg_downtime_pct").cast(pl.Decimal(6, 3)),
            pl.col("best_day_oil").cast(pl.Decimal(12, 3)),
            pl.col("worst_day_oil").cast(pl.Decimal(12, 3)),
            pl.col("production_rank"),
            pl.col("performance_group").cast(pl.String),
        ]
    )
