from typing import Dict, Any

import polars as pl


def aggregate_event_time_metrics(
    lf: pl.LazyFrame, dataset: str, aggregation_rules: Dict[str, Any]
) -> pl.LazyFrame:
    """
    Выполняет временные агрегации (rollups), например, посуточное усреднение данных телеметрии.
    Группирует данные по идентификатору сущности и усеченному значению времени.
    """
    if not aggregation_rules:
        return lf

    key_col = aggregation_rules["key"]
    time_col = aggregation_rules["time_column"]
    granularity = aggregation_rules.get("granularity", "1d")
    metrics = aggregation_rules["metrics"]

    # Определение выражения для группировки по времени (например, приведение к дате)
    if granularity == "1d":
        group_time_expr = pl.col(time_col).dt.date().alias("event_date")
    else:
        group_time_expr = (
            pl.col(time_col).dt.truncate(granularity).alias(f"event_{granularity}")
        )

    # Формирование списка выражений для расчета метрик
    agg_exprs = []
    for col, agg_funcs in metrics.items():
        for func in agg_funcs:
            if func == "mean":
                agg_exprs.append(pl.col(col).mean().alias(f"avg_{col}"))
            elif func == "max":
                agg_exprs.append(pl.col(col).max().alias(f"max_{col}"))
            elif func == "min":
                agg_exprs.append(pl.col(col).min().alias(f"min_{col}"))
            elif func == "sum":
                agg_exprs.append(pl.col(col).sum().alias(f"sum_{col}"))

    # Выполнение группировки и агрегации
    lf_agg = lf.group_by([key_col, group_time_expr]).agg(agg_exprs)

    return lf_agg
