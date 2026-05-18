# plugins/bronze_to_silver/normalizer.py
from datetime import datetime, timezone
from typing import Any, Dict

import polars as pl


def normalize_dataset(
    lf: pl.LazyFrame, dataset: str, schema_contract: Dict[str, Any]
) -> pl.LazyFrame:

    expected_schema = schema_contract["columns"]
    expressions = []

    for col_name, dtype in expected_schema.items():
        expr = pl.col(col_name)

        expr = expr.cast(dtype)

        if dtype.base_type() == pl.Datetime:
            expr = expr.cast(pl.Datetime("us"))

        elif dtype.base_type() in (pl.Float32, pl.Float64):
            expr = (
                pl.when(expr.is_nan() | expr.is_infinite()).then(None).otherwise(expr)
            )

        elif dtype.base_type() == pl.String:
            expr = expr.str.strip_chars()

        expressions.append(expr.alias(col_name))

    lf = lf.select(expressions).with_columns(
        pl.lit(datetime.now(timezone.utc))
        .dt.cast_time_unit("us")
        .alias("_silver_processed_at")
    )

    return lf
