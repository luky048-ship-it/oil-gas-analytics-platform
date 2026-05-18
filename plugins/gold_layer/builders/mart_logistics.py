import polars as pl


def build_mart_logistics(
    lf_deliv: pl.LazyFrame, lf_drivers: pl.LazyFrame, lf_vehicles: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Формирует витрину логистических показателей (mart_logistics).
    Обогащает данные о поставках информацией о водителях и транспортных средствах.
    Рассчитывает удельные затраты на километр и на тонну груза.
    """
    # 1. Обогащение данных о доставках справочной информацией
    df = lf_deliv.join(lf_drivers, on="driver_id", how="left").join(
        lf_vehicles, on="vehicle_id", how="left"
    )

    # 2. Расчет производных логистических метрик
    df = df.with_columns(
        [
            (pl.col("cost_usd") / pl.col("distance_km")).alias("cost_per_km"),
            (pl.col("cost_usd") / pl.col("volume_ton")).alias("cost_per_ton"),
            (pl.col("delay_hours") > 0).alias("delay_flag"),
        ]
    )

    # 3. Приведение типов данных к целевой схеме Postgres
    return df.select(
        [
            pl.col("delivery_id").cast(pl.Int64),
            pl.col("date").cast(pl.Date),
            pl.col("source").cast(pl.String),
            pl.col("destination").cast(pl.String),
            pl.col("product_type").cast(pl.String),
            pl.col("volume_ton").cast(pl.Decimal(12, 3)),
            pl.col("cost_usd").cast(pl.Decimal(14, 2)),
            pl.col("delay_hours").cast(pl.Decimal(8, 2)),
            pl.col("distance_km").cast(pl.Decimal(10, 2)),
            pl.col("weather_conditions").cast(pl.String),
            pl.col("driver_id").cast(pl.Int32),
            pl.col("name").alias("driver_name").cast(pl.String),
            pl.col("experience_years").cast(pl.Int32),
            pl.col("vehicle_id").cast(pl.Int32),
            pl.col("plate_number").cast(pl.String),
            pl.col("capacity_ton").cast(pl.Decimal(8, 2)),
            pl.col("fuel_type").cast(pl.String),
            pl.col("cost_per_km").cast(pl.Decimal(10, 2)),
            pl.col("cost_per_ton").cast(pl.Decimal(10, 2)),
            pl.col("delay_flag").cast(pl.Boolean),
        ]
    )
