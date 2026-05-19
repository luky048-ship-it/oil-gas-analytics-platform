# plugins/bronze_to_silver/missing_handler.py
from typing import Dict, Any

import polars as pl


def handle_missing_values(
    lf: pl.LazyFrame, dataset: str, missing_rules: Dict[str, Dict[str, Any]]
) -> pl.LazyFrame:
    """
    Fills missing values based on contract rules (fill_value or forward_fill).
    Для forward_fill используется сортировка внутри оконной функции через sort_by.
    """
    if not missing_rules:
        return lf

    expressions = []

    for col, rule in missing_rules.items():
        strategy = rule["strategy"]

        if strategy == "fill_value":
            expr = pl.col(col).fill_null(rule["value"])
            expressions.append(expr.alias(col))

        elif strategy == "forward_fill":
            partition_col = rule["partition_by"]
            order_col = rule["order_by"]
            # Сортировка указывается напрямую внутри оконного вызова
            # Это гарантирует корректный порядок при параллельном выполнении
            expr = pl.col(col).sort_by(order_col).forward_fill().over(partition_col)
            expressions.append(expr.alias(col))

    if expressions:
        lf = lf.with_columns(expressions)

    return lf
