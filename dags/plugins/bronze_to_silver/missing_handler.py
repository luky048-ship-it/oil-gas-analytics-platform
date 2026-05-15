# plugins/bronze_to_silver/missing_handler.py
from typing import Dict, Any

import polars as pl


def handle_missing_values(
    lf: pl.LazyFrame, dataset: str, missing_rules: Dict[str, Dict[str, Any]]
) -> pl.LazyFrame:
    """
    Fills missing values based on contract rules (fill_value or forward_fill).
    """
    if not missing_rules:
        return lf

    expressions = []

    # We need to pre-sort if forward_fill requires ordering
    sort_required = False
    sort_cols = []

    for col, rule in missing_rules.items():
        if rule["strategy"] == "forward_fill":
            sort_required = True
            sort_cols = [rule["partition_by"], rule["order_by"]]
            break

    if sort_required:
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
