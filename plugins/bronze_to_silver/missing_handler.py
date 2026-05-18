from typing import Dict, Any

import polars as pl


def handle_missing_values(
    lf: pl.LazyFrame, dataset: str, missing_rules: Dict[str, Dict[str, Any]]
) -> pl.LazyFrame:
    """
    Обрабатывает пропущенные значения (NULL) в соответствии с правилами контракта.
    Поддерживает заполнение константой (fill_value) или методом переноса последнего значения (forward_fill).
    """
    if not missing_rules:
        return lf

    expressions = []

    # Проверка необходимости предварительной сортировки для метода forward_fill
    sort_required = False
    sort_cols = []

    for col, rule in missing_rules.items():
        if rule["strategy"] == "forward_fill":
            sort_required = True
            sort_cols = [rule["partition_by"], rule["order_by"]]
            break

    # Выполнение сортировки по ключам и времени при необходимости
    if sort_required:
        lf = lf.sort(sort_cols)

    # Формирование выражений для обработки пропусков по каждому столбцу
    for col, rule in missing_rules.items():
        strategy = rule["strategy"]

        if strategy == "fill_value":
            expr = pl.col(col).fill_null(rule["value"])
            expressions.append(expr.alias(col))

        elif strategy == "forward_fill":
            partition_col = rule["partition_by"]
            # Применение оконной функции для заполнения пропусков внутри группы
            expr = pl.col(col).forward_fill().over(partition_col)
            expressions.append(expr.alias(col))

    if expressions:
        lf = lf.with_columns(expressions)

    return lf
