# /plugins/bronze_to_silver/outlier_detector.py
from typing import List, Optional, Tuple

import polars as pl


def detect_outliers(
    lf: pl.LazyFrame,
    monitored_columns: List[str],
    group_by_col: Optional[str] = None,
    method: str = "iqr",
    multiplier: float = 6.0,
    min_samples: int = 15,
) -> Tuple[pl.LazyFrame, Optional[pl.LazyFrame]]:
    """
    Обнаруживает экстремальные аномалии на основе группового IQR.

    Сохраняет жесткую изоляцию данных:
    - Чистые данные -> valid_lf (Silver)
    - Экстремальные выбросы -> invalid_lf (Карантин)
    """
    if not monitored_columns:
        return lf, lf.filter(pl.lit(False))

    outlier_conditions = []
    schema = lf.collect_schema()

    if method == "iqr":
        for col in monitored_columns:
            if col not in schema:
                continue

            col_expr = pl.col(col)

            if group_by_col and group_by_col in schema:
                group_size = pl.len().over(group_by_col)  # Совместимо с Polars 1.4

                q1 = col_expr.quantile(0.25).over(group_by_col)
                q3 = col_expr.quantile(0.75).over(group_by_col)
                iqr = q3 - q1

                lower_bound = q1 - (multiplier * iqr)
                upper_bound = q3 + (multiplier * iqr)

                condition = (group_size >= min_samples) & (
                    (col_expr < lower_bound) | (col_expr > upper_bound)
                )
            else:
                total_size = pl.len()
                q1 = col_expr.quantile(0.25)
                q3 = col_expr.quantile(0.75)
                iqr = q3 - q1

                lower_bound = q1 - (multiplier * iqr)
                upper_bound = q3 + (multiplier * iqr)

                condition = (total_size >= min_samples) & (
                    (col_expr < lower_bound) | (col_expr > upper_bound)
                )

            outlier_conditions.append(condition)

    if not outlier_conditions:
        return lf, lf.filter(pl.lit(False))

    is_outlier_expr = pl.any_horizontal(*outlier_conditions).fill_null(False)

    valid_lf = lf.filter(~is_outlier_expr)
    invalid_lf = lf.filter(is_outlier_expr)

    return valid_lf, invalid_lf
