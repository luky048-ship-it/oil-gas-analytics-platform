from datetime import datetime, timezone

import polars as pl


def build_mart_production(
    lf_production: pl.LazyFrame, lf_telemetry: pl.LazyFrame, lf_targets: pl.LazyFrame
) -> pl.LazyFrame:

    # 1. Агрегация телеметрии
    telemetry_daily = (
        lf_telemetry.with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by(["well_id", "date"])
        .agg(
            [
                pl.col("temperature").mean().alias("avg_temperature"),
                # В витрине avg_pressure, берем давление на выходе (out)
                pl.col("pressure_out").mean().alias("avg_pressure"),
                pl.col("pump_speed_rpm").mean().alias("avg_pump_speed_rpm"),
                pl.col("oil_flow_rate").mean().alias("avg_oil_flow_rate"),
                pl.col("vibration").max().alias("max_vibration"),
            ]
        )
    )

    # 2. Сборка витрины
    mart_lf = (
        lf_production.join(telemetry_daily, on=["well_id", "date"], how="left").join(
            lf_targets, on=["well_id", "date"], how="left"
        )
        # Исправляем имя: в источнике daily_oil_ton -> в таргете daily_target_ton
        .rename({"daily_oil_ton": "daily_target_ton"})
    )

    # 3. Расчет KPI и добавление технических колонок
    mart_lf = mart_lf.with_columns(
        [
            (pl.col("oil_ton") / pl.col("daily_target_ton")).alias(
                "production_efficiency"
            ),
            (pl.col("downtime_hours") / 24.0).alias("downtime_pct"),
            pl.lit(datetime.now(timezone.utc)).alias("load_timestamp"),
            pl.col("date").alias("partition_date"),
        ]
    )

    # 4. ФИНАЛЬНЫЙ КАСТИНГ
    return mart_lf.select(
        [
            pl.col("well_id").cast(pl.Int32),
            pl.col("date").cast(pl.Date),
            pl.col("oil_ton").cast(pl.Decimal(12, 3)),
            pl.col("gas_m3").cast(pl.Decimal(14, 2)),
            pl.col("water_m3").cast(pl.Decimal(14, 2)),
            pl.col("energy_kwh").cast(pl.Decimal(14, 2)),
            pl.col("downtime_hours").cast(pl.Decimal(6, 2)),
            pl.col("avg_temperature").cast(pl.Decimal(6, 2)),
            pl.col("avg_pressure").cast(pl.Decimal(8, 2)),
            pl.col("avg_pump_speed_rpm").cast(pl.Decimal(10, 2)),
            pl.col("avg_oil_flow_rate").cast(pl.Decimal(10, 3)),
            pl.col("max_vibration").cast(pl.Decimal(6, 2)),
            pl.col("daily_target_ton").cast(pl.Decimal(12, 3)),
            pl.col("production_efficiency").cast(pl.Decimal(8, 4)),
            pl.col("downtime_pct").cast(pl.Decimal(6, 3)),
            pl.col("load_timestamp").cast(pl.Datetime("us")),
            pl.col("partition_date").cast(pl.Date),
        ]
    )
