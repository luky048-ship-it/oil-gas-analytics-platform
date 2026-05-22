# plugins/bronze_to_silver/missing_handler.py
from typing import Any, Dict, List

import polars as pl


def handle_missing_values(
    lf: pl.LazyFrame, missing_rules: Dict[str, Dict[str, Any]]
) -> pl.LazyFrame:
    """
    Заполняет пропущенные значения на основе правил контракта (fill_value или forward_fill).
    """
    if not missing_rules:
        return lf

    expressions = []
    sort_cols: List[str] = []

    for col, rule in missing_rules.items():
        if rule["strategy"] == "forward_fill":
            partition_col = rule["partition_by"]
            order_col = rule["order_by"]
            if partition_col not in sort_cols:
                sort_cols.append(partition_col)
            if order_col not in sort_cols:
                sort_cols.append(order_col)

    if sort_cols:
        lf = lf.sort(sort_cols)

    for col, rule in missing_rules.items():
        strategy = rule["strategy"]

        if strategy == "fill_value":
            expr = pl.col(col).fill_null(rule["value"])
            expressions.append(expr.alias(col))
        elif strategy == "forward_fill":
            partition_col = rule["partition_by"]
            expr = pl.col(col).forward_fill().over(partition_col)
            expressions.append(expr.alias(col))

    if expressions:
        lf = lf.with_columns(expressions)

    return lf
