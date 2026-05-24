# /plugins/bronze_to_silver/normalizer.py
from datetime import datetime, timezone
from typing import Any, Dict

import polars as pl


def normalize_dataset(
    lf: pl.LazyFrame, schema_contract: Dict[str, Any]
) -> pl.LazyFrame:
    """
    Нормализует типы данных, стандартизирует временные метки (UTC, обрезает до секунд)
    и преобразует NaN/Inf в null для колонок с плавающей точкой.
    Добавляет технические колонки для отслеживания обработки.
    """
    expected_schema = schema_contract["columns"]
    expressions = []

    for col_name, dtype in expected_schema.items():
        expr = pl.col(col_name).cast(dtype)

        if dtype.base_type() == pl.Datetime:
            expr = expr.dt.truncate("1s")
        elif dtype.is_float():
            expr = (
                pl.when(expr.is_nan() | expr.is_infinite()).then(None).otherwise(expr)
            )
        elif dtype.base_type() == pl.String:
            expr = expr.str.strip_chars()

        expressions.append(expr.alias(col_name))

    lf = lf.select(expressions).with_columns(
        pl.lit(datetime.now(timezone.utc), dtype=pl.Datetime("ms", "UTC")).alias(
            "_silver_processed_at"
        )
    )

    return lf
