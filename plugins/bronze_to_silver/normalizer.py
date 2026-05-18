from datetime import datetime, timezone
from typing import Any, Dict

import polars as pl


def normalize_dataset(
    lf: pl.LazyFrame, dataset: str, schema_contract: Dict[str, Any]
) -> pl.LazyFrame:
    """
    Выполняет нормализацию набора данных в соответствии с контрактом схемы.
    Включает приведение типов, обработку аномальных значений (NaN/Inf) и очистку строк.
    """
    expected_schema = schema_contract["columns"]
    expressions = []

    # Формирование выражений для трансформации каждого столбца
    for col_name, dtype in expected_schema.items():
        expr = pl.col(col_name)

        # Базовое приведение типов
        expr = expr.cast(dtype)

        # Специальная обработка для временных меток
        if dtype.base_type() == pl.Datetime:
            expr = expr.cast(pl.Datetime("us"))

        # Обработка некорректных значений для чисел с плавающей точкой
        elif dtype.base_type() in (pl.Float32, pl.Float64):
            expr = (
                pl.when(expr.is_nan() | expr.is_infinite()).then(None).otherwise(expr)
            )

        # Очистка строковых значений от лишних пробелов
        elif dtype.base_type() == pl.String:
            expr = expr.str.strip_chars()

        expressions.append(expr.alias(col_name))

    # Добавление технического столбца с временем обработки записи
    lf = lf.select(expressions).with_columns(
        pl.lit(datetime.now(timezone.utc))
        .dt.cast_time_unit("us")
        .alias("_silver_processed_at")
    )

    return lf
