import polars as pl


def build_mart_failures(
    lf_sensors: pl.LazyFrame, lf_failures: pl.LazyFrame, lf_pumps: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Формирует витрину анализа отказов оборудования (mart_failures).
    Объединяет телеметрию насосов со справочной информацией и историей отказов.
    Рассчитывает статистические аномалии (Z-Score) для вибрации и температуры.
    """
    # 1. Обогащение телеметрии датчиков данными о характеристиках насосов
    df = lf_sensors.join(lf_pumps, on="pump_id", how="left")

    # 2. Расчет статистических показателей (Z-Score) для выявления отклонений
    df = df.with_columns(
        [
            (
                (pl.col("vibration") - pl.col("vibration").mean().over("pump_id"))
                / pl.col("vibration").std().over("pump_id")
            ).alias("vibration_zscore"),
            (
                (pl.col("temperature") - pl.col("temperature").mean().over("pump_id"))
                / pl.col("temperature").std().over("pump_id")
            ).alias("temperature_zscore"),
        ]
    )

    # 3. Маркировка строк как аномальных при существенном отклонении от среднего
    df = df.with_columns(
        pl.any_horizontal(
            [
                pl.col("vibration_zscore").abs() > 3,
                pl.col("temperature_zscore").abs() > 3,
            ]
        ).alias("is_anomaly")
    )

    # 4. Сопоставление данных телеметрии с фактическими событиями отказов
    lf_fail_event = lf_failures.select(
        [
            "pump_id",
            pl.col("failure_date").alias("timestamp"),
            "failure_type",
            pl.lit(True).alias("is_failure"),
        ]
    )

    df = df.join(lf_fail_event, on=["pump_id", "timestamp"], how="left")
    df = df.with_columns(
        pl.col("date").alias("partition_date")
    )

    # 5. Финальное приведение типов данных для экспорта в Gold-слой
    return df.select(
        [
            pl.col("pump_id").cast(pl.Int32),
            pl.col("well_id").cast(pl.Int32),
            pl.col("timestamp").dt.date().alias("date").cast(pl.Date),
            pl.col("timestamp").cast(pl.Datetime("us")),
            pl.col("temperature").cast(pl.Decimal(6, 2)),
            pl.col("vibration").cast(pl.Decimal(6, 2)),
            pl.col("current").cast(pl.Decimal(8, 2)),
            pl.col("rpm").cast(pl.Decimal(10, 2)),
            pl.col("pressure").cast(pl.Decimal(8, 2)),
            pl.col("vibration_zscore").cast(pl.Decimal(6, 3)),
            pl.col("temperature_zscore").cast(pl.Decimal(6, 3)),
            pl.col("is_anomaly").cast(pl.Boolean),
            pl.col("failure_type").cast(pl.String),
            pl.col("is_failure").fill_null(False).cast(pl.Boolean),
            pl.lit(0.05).alias("risk_score").cast(pl.Decimal(5, 4)),
            pl.lit(0.01).alias("failure_probability").cast(pl.Decimal(5, 4)),
        ]
    )
