# plugins/bronze_to_silver/missing_handler.py
from typing import Dict, Any

import polars as pl


def handle_missing_values(
    lf: pl.LazyFrame, dataset: str, missing_rules: Dict[str, Dict[str, Any]]
) -> pl.LazyFrame:
    """
    Fills missing values based on contract rules (fill_value or forward_fill).
    Для forward_fill используется сортировка внутри оконной функции через sort_by.
    Важно: при fill_value сохраняется тип колонки через явное приведение литерала.
    """
    if not missing_rules:
        return lf

    expressions = []
    # Получаем схему ДО любых модификаций для корректного приведения типов
    current_schema = lf.collect_schema()

    for col, rule in missing_rules.items():
        strategy = rule["strategy"]

        if strategy == "fill_value":
            fill_val = rule["value"]
            target_dtype = current_schema.get(col)
            
            # Строго оборачиваем значение в литерал с нужным типом
            # Это предотвращает неявный upcast Decimal -> Float64
            if target_dtype:
                expr = pl.col(col).fill_null(pl.lit(fill_val, dtype=target_dtype))
            else:
                expr = pl.col(col).fill_null(fill_val)
                
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
