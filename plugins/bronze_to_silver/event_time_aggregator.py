# plugins/bronze_to_silver/event_time_aggregator.py
from typing import Any, Dict

import polars as pl


def aggregate_event_time_metrics(
    lf: pl.LazyFrame, dataset: str, aggregation_rules: Dict[str, Any]
) -> pl.LazyFrame:
    """
    Performs event-time rollups (e.g., daily aggregation for telemetry).
    Groups by the specified entity key and truncated time column.
    Важно: агрегированная колонка времени сохраняет имя исходной колонки (time_col),
    чтобы последующие шаги пайплайна могли корректно обращаться к ней.
    """
    if not aggregation_rules:
        return lf

    key_col = aggregation_rules["key"]
    time_col = aggregation_rules["time_column"]
    granularity = aggregation_rules.get("granularity", "1d")
    metrics = aggregation_rules["metrics"]

    # Use collect_schema() to avoid PerformanceWarning
    schema = lf.collect_schema()
    dtype = schema[time_col]

    if granularity == "1d":
        if dtype == pl.Date:
            group_time_expr = pl.col(time_col).alias(time_col)
        else:
            group_time_expr = pl.col(time_col).dt.date().alias(time_col)

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

    # Preserve technical audit column through aggregation
    if "_silver_processed_at" in lf.collect_schema():
        agg_exprs.append(pl.col("_silver_processed_at").max().alias("_silver_processed_at"))
        
    lf_agg = lf.group_by([key_col, group_time_expr]).agg(agg_exprs)

    return lf_agg
