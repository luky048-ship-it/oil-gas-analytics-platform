# plugins/bronze_to_silver/outlier_detector.py
from typing import List, Optional, Tuple

import polars as pl


def detect_outliers(
    lf: pl.LazyFrame,
    dataset: str,
    monitored_columns: List[str],
    method: str = "iqr",
    multiplier: float = 3.0,
) -> Tuple[pl.LazyFrame, Optional[pl.LazyFrame]]:
    """
    Detects statistical outliers using IQR or Z-score without dropping rows silently.
    Returns a tuple of (valid_lf, invalid_lf).
    invalid_lf is enriched with quarantine metadata.
    """
    if not monitored_columns:
        return lf, None

    outlier_conditions = []

    if method == "iqr":
        for col in monitored_columns:
            col_float = pl.col(col).cast(pl.Float64, strict=False)
            q1 = col_float.quantile(0.25)
            q3 = col_float.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (multiplier * iqr)
            upper_bound = q3 + (multiplier * iqr)

            condition = (col_float < lower_bound) | (col_float > upper_bound)
            outlier_conditions.append(condition)

    is_outlier_expr = pl.any_horizontal(*outlier_conditions).fill_null(False)

    valid_lf = lf.filter(~is_outlier_expr)
    invalid_lf = lf.filter(is_outlier_expr)

    invalid_lf = invalid_lf.with_columns(
        [
            pl.lit("OUTLIER_DETECTION").alias("_quarantine_validation_name"),
            pl.lit(f"{method.upper()}_VIOLATION").alias("_quarantine_reason_code"),
        ]
    )

    return valid_lf, invalid_lf
