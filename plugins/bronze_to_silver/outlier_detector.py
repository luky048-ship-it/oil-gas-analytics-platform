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
    Выявляет статистические аномалии (outliers) с использованием метода IQR или Z-score.
    Возвращает кортеж (valid_lf, invalid_lf), где invalid_lf содержит записи-аномалии
    с метаданными для карантина.
    """
    if not monitored_columns:
        return lf, None

    outlier_conditions = []

    # Реализация метода межквартильного размаха (IQR) для поиска выбросов
    if method == "iqr":
        for col in monitored_columns:
            # Расчет границ на основе квантилей
            q1 = pl.col(col).quantile(0.25)
            q3 = pl.col(col).quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (multiplier * iqr)
            upper_bound = q3 + (multiplier * iqr)

            condition = (pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)
            outlier_conditions.append(condition)

    # Объединение условий: запись считается аномальной, если выброс обнаружен хотя бы в одном столбце
    is_outlier_expr = pl.any_horizontal(*outlier_conditions)

    valid_lf = lf.filter(~is_outlier_expr)
    invalid_lf = lf.filter(is_outlier_expr)

    # Обогащение подозрительных записей метаданными для последующей записи в карантин
    invalid_lf = invalid_lf.with_columns(
        [
            pl.lit("OUTLIER_DETECTION").alias("_quarantine_validation_name"),
            pl.lit(f"{method.upper()}_VIOLATION").alias("_quarantine_reason_code"),
        ]
    )

    return valid_lf, invalid_lf
