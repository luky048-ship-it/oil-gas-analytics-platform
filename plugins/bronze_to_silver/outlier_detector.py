# /plugins/bronze_to_silver/outlier_detector.py
from typing import List, Optional, Tuple

import polars as pl


def detect_outliers(
    lf: pl.LazyFrame,
    monitored_columns: List[str],
    method: str = "iqr",
    multiplier: float = 3.0,
) -> Tuple[pl.LazyFrame, Optional[pl.LazyFrame]]:
    """
    Обнаруживает статистические выбросы с использованием метода IQR.
    """
    if not monitored_columns:
        return lf, None

    outlier_conditions = []

    if method == "iqr":
        for col in monitored_columns:
            q1 = pl.col(col).quantile(0.25)
            q3 = pl.col(col).quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (multiplier * iqr)
            upper_bound = q3 + (multiplier * iqr)

            condition = (pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)
            outlier_conditions.append(condition)

    is_outlier_expr = pl.any_horizontal(*outlier_conditions).fill_null(False)

    valid_lf = lf.filter(~is_outlier_expr)
    invalid_lf = lf.filter(is_outlier_expr)

    return valid_lf, invalid_lf
