# plugins/bronze_to_silver/normalizer.py
from datetime import datetime, timezone
from typing import Any, Dict

import polars as pl


def normalize_dataset(
    lf: pl.LazyFrame, dataset: str, schema_contract: Dict[str, Any]
) -> pl.LazyFrame:
    """
    Normalizes data types, standardizes timestamps (UTC, truncates to seconds),
    and converts NaN/Inf to nulls for float columns.
    Adds technical lineage columns.
    """
    expected_schema = schema_contract["columns"]
    expressions = []

    for col_name, dtype in expected_schema.items():
        expr = pl.col(col_name)

        # Cast to expected base type
        expr = expr.cast(dtype)

        # Standardize timestamps: ensure UTC and truncate to seconds
        if dtype.base_type() == pl.Datetime:
            expr = expr.dt.truncate("1s")

        # Handle NaN and Infinity for floats
        elif dtype.base_type() in (pl.Float32, pl.Float64):
            expr = (
                pl.when(expr.is_nan() | expr.is_infinite()).then(None).otherwise(expr)
            )

        # Standardize strings: strip whitespaces
        elif dtype.base_type() == pl.String:
            expr = expr.str.strip_chars()

        expressions.append(expr.alias(col_name))

    # Apply normalizations and add lineage column
    lf = lf.select(expressions).with_columns(
        pl.lit(datetime.now(timezone.utc))
        .dt.truncate("1s")
        .dt.cast_time_unit("ms")
        .alias("_silver_processed_at")
    )

    return lf
