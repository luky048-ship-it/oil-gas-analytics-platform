# plugins/gold_layer/generic_builder.py
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import polars as pl
from gold_layer.config import (ANALYSIS_PARAMS, ColumnMapping, DerivedColumn,
                               JoinSpec, MartSpec, WindowAggregation)

logger = logging.getLogger(__name__)


def _fix_adbc_numerics(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Устраняет поведение adbc-driver-postgresql, при котором NUMERIC
    считывается как pl.String. Автоматически приводит числовые колонки
    обратно к Float64 на базе эвристики имен.
    """
    schema = lf.collect_schema()
    cast_exprs = []

    str_keywords = {
        "name",
        "region",
        "operator",
        "status",
        "type",
        "manufacturer",
        "model",
        "source",
        "destination",
        "conditions",
        "plate",
        "fuel",
        "impact",
        "reason",
        "group",
        "id",
        "dag_run",
        "path",
        "hash",
    }

    for col_name, dtype in schema.items():
        if dtype == pl.String:
            if not any(k in col_name.lower() for k in str_keywords):
                logger.info(
                    "Auto-casting likely ADBC-stringified NUMERIC column '%s' to Float64",
                    col_name,
                )
                cast_exprs.append(pl.col(col_name).cast(pl.Float64, strict=False))

    if cast_exprs:
        return lf.with_columns(cast_exprs)
    return lf


def _get_max_window_days(spec: MartSpec) -> int:
    max_days = 0
    for win in spec.window_aggregations:
        win_val = ANALYSIS_PARAMS.get(win.window_expr, 0)
        days = win_val / (24 * 60) if win_val > 1000 else win_val
        max_days = max(max_days, int(days))
    return max_days


def _prepare_and_concat_sources(
    spec: MartSpec,
    lf_dict: Dict[str, Optional[pl.LazyFrame]],
) -> tuple[pl.LazyFrame, Optional[date]]:
    joined_tables = {j.right_table for j in spec.joins}
    base_tables = [t for t in spec.source_tables if t not in joined_tables]

    if not base_tables:
        raise ValueError(f"No base tables found for mart '{spec.table_name}'")

    batch_lfs = []
    hist_lfs = []
    min_batch_date = None

    for t in base_tables:
        if t not in lf_dict or lf_dict[t] is None:
            logger.warning("Base table '%s' not found or empty, skipping", t)
            continue

        lf = lf_dict[t]
        schema = lf.collect_schema().names()

        if "date" not in schema and "timestamp" in schema:
            lf = lf.with_columns(pl.col("timestamp").dt.date().alias("date"))
            schema.append("date")

        if "history" in t.lower():
            hist_lfs.append((t, lf))
        else:
            batch_lfs.append(lf)
            if "date" in schema:
                t_min = lf.select(pl.col("date").min()).collect().item()
                if t_min:
                    if not min_batch_date or t_min < min_batch_date:
                        min_batch_date = t_min

    if not batch_lfs:
        raise RuntimeError("No active batch data provided for mart.")

    max_window = _get_max_window_days(spec)
    if min_batch_date and max_window > 0 and hist_lfs:
        safe_min_date = min_batch_date - timedelta(days=max_window + 1)
        logger.info(
            "Pushdown history filter applied: >= %s (window=%d days)",
            safe_min_date,
            max_window,
        )
        for name, h_lf in hist_lfs:
            if "date" in h_lf.collect_schema().names():
                batch_lfs.append(h_lf.filter(pl.col("date") >= safe_min_date))
            else:
                batch_lfs.append(h_lf)
    else:
        for _, h_lf in hist_lfs:
            batch_lfs.append(h_lf)

    combined = pl.concat(batch_lfs, how="diagonal_relaxed")
    return combined, min_batch_date


def _apply_joins(
    lf: pl.LazyFrame, joins: List[JoinSpec], lf_dict: Dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    for i, js in enumerate(joins):
        right_lf = lf_dict.get(js.right_table)
        if right_lf is None:
            logger.warning(
                "Join #%d skipped: right table '%s' not found", i, js.right_table
            )
            continue

        logger.info("Applying join #%d: right=%s, how=%s", i, js.right_table, js.how)

        left_schema = lf.collect_schema()
        right_schema = right_lf.collect_schema()

        left_cols = set(left_schema.names())
        right_cols_to_drop = [
            c for c in right_schema.names() if c in left_cols and c not in js.right_on
        ]
        if right_cols_to_drop:
            right_lf = right_lf.drop(right_cols_to_drop)

        for l_col, r_col in zip(js.left_on, js.right_on):
            l_type = left_schema[l_col]
            r_type = right_schema[r_col]
            if l_type != r_type:
                if l_type.is_integer() and r_type.is_integer():
                    lf = lf.with_columns(pl.col(l_col).cast(pl.Int64))
                    right_lf = right_lf.with_columns(pl.col(r_col).cast(pl.Int64))
                elif l_type.is_float() or r_type.is_float():
                    lf = lf.with_columns(pl.col(l_col).cast(pl.Float64))
                    right_lf = right_lf.with_columns(pl.col(r_col).cast(pl.Float64))

        lf = lf.join(right_lf, left_on=js.left_on, right_on=js.right_on, how=js.how)

    return lf


def _build_aggregation(
    lf: pl.LazyFrame, group_by: List[str], aggregations: List[ColumnMapping]
) -> pl.LazyFrame:
    if not group_by or not aggregations:
        return lf

    logger.info("Aggregation: group_by=%s, metrics=%d", group_by, len(aggregations))

    agg_exprs = []
    for cm in aggregations:
        func = cm.agg_func or "first"
        col_expr = pl.col(cm.source_col).drop_nulls()

        if func == "zscore":
            raise ValueError("zscore is only supported in window_aggregations")

        expr = col_expr.first() if func == "first" else getattr(col_expr, func)()

        if cm.default is not None:
            expr = expr.fill_null(cm.default)

        if func in ("sum", "mean") or isinstance(cm.default, float):
            expr = expr.cast(pl.Float64, strict=False)

        agg_exprs.append(expr.alias(cm.target))

    return lf.group_by(group_by).agg(agg_exprs)


def _apply_window_aggregations(
    lf: pl.LazyFrame, window_aggs: List[WindowAggregation], params: Dict[str, Any]
) -> pl.LazyFrame:
    if not window_aggs:
        return lf

    unique_sort_cols = list(
        dict.fromkeys(
            [col for win in window_aggs for col in win.partition_by + [win.order_by]]
        )
    )
    lf = lf.sort(unique_sort_cols)

    window_exprs = []
    for win in window_aggs:
        raw_size = params.get(win.window_expr, win.window_expr)

        if "kpi" in win.window_expr.lower():
            window_size_rows = int(raw_size)
        elif "risk" in win.window_expr.lower():
            logger.warning(
                "Converting minute-based window to estimated row-count (56 rows for 7 days)."
            )
            window_size_rows = 56
        else:
            window_size_rows = int(raw_size)

        col_expr = pl.col(win.source_col)

        if win.agg_func in ("mean", "sum", "max", "min", "zscore"):
            col_expr = col_expr.cast(pl.Float64, strict=False)

        if win.agg_func in ("mean", "sum", "max", "min"):
            expr = getattr(col_expr, f"rolling_{win.agg_func}")(
                window_size=window_size_rows, min_periods=1
            )
        elif win.agg_func == "zscore":
            mean_expr = col_expr.rolling_mean(
                window_size=window_size_rows, min_periods=1
            )
            std_expr = (
                col_expr.rolling_std(window_size=window_size_rows, min_periods=1)
                .fill_nan(0.0)
                .fill_null(0.0)
            )
            expr = (
                pl.when(std_expr > 0.0)
                .then((col_expr - mean_expr) / std_expr)
                .otherwise(0.0)
            )
        else:
            raise ValueError(f"Unsupported window aggregation: {win.agg_func}")

        expr = expr.over(win.partition_by, order_by=win.order_by).alias(win.target)
        window_exprs.append(expr)

    return lf.with_columns(window_exprs)


def _derive_columns(lf: pl.LazyFrame, derivations: List[DerivedColumn]) -> pl.LazyFrame:
    if not derivations:
        return lf

    safe_env = {"pl": pl, "ANALYSIS_PARAMS": ANALYSIS_PARAMS}

    for dc in derivations:
        logger.info("Derived column: %s", dc.target)
        try:
            expr = eval(dc.expression, {"__builtins__": {}}, safe_env)
        except Exception as e:
            raise ValueError(f"Evaluation failed for '{dc.target}': {e}") from e

        if dc.condition:
            try:
                cond_expr = eval(dc.condition, {"__builtins__": {}}, safe_env)
                expr = pl.when(cond_expr).then(expr).otherwise(pl.lit(None))
            except Exception as e:
                raise ValueError(
                    f"Condition evaluation failed for '{dc.target}': {e}"
                ) from e

        lf = lf.with_columns(expr.alias(dc.target))

    return lf


def _apply_output_schema(lf: pl.LazyFrame, spec: MartSpec) -> pl.LazyFrame:
    output_schema = spec.output_schema
    current_cols = set(lf.collect_schema().names())

    if "driver_name" in output_schema and "driver_name" not in current_cols:
        if "name" in current_cols:
            logger.info(
                "Dynamically mapping source 'name' to 'driver_name' to fix cache lag for '%s'",
                spec.table_name,
            )
            lf = lf.with_columns(pl.col("name").alias("driver_name"))
            current_cols.add("driver_name")

    if "load_timestamp" in output_schema and "load_timestamp" not in current_cols:
        lf = lf.with_columns(
            pl.lit(datetime.now(timezone.utc))
            .dt.replace_time_zone(None)
            .alias("load_timestamp")
        )
        current_cols.add("load_timestamp")

    if "partition_date" in output_schema and "partition_date" not in current_cols:
        pc = spec.partition_column
        if pc and pc in current_cols:
            lf = lf.with_columns(pl.col(pc).cast(pl.Date).alias("partition_date"))
        elif "date" in current_cols:
            lf = lf.with_columns(pl.col("date").cast(pl.Date).alias("partition_date"))
        else:
            lf = lf.with_columns(pl.lit(None).cast(pl.Date).alias("partition_date"))
        current_cols.add("partition_date")

    for col, dtype in output_schema.items():
        if col not in current_cols:
            if col == "mart_id" and spec.primary_key:
                lf = lf.with_columns(
                    pl.concat_str([pl.col(c) for c in spec.primary_key])
                    .hash(seed=42)
                    .cast(dtype, strict=False)
                    .abs()
                    .alias(col)
                )
                current_cols.add(col)
            elif col == "record_id":
                lf = lf.with_columns(
                    pl.int_range(0, pl.len()).cast(dtype, strict=False).alias(col)
                )
                current_cols.add(col)
            else:
                raise ValueError(
                    f"CRITICAL: Missing column '{col}' in mart '{spec.table_name}'"
                )

    select_exprs = [
        pl.col(col).cast(dtype, strict=False) for col, dtype in output_schema.items()
    ]
    return lf.select(select_exprs)


def build_mart(
    spec: MartSpec,
    lf_dict: Dict[str, pl.LazyFrame],
    params: Optional[Dict[str, Any]] = None,
) -> pl.LazyFrame:
    """
    Главный оркестратор построения витрины.
    """
    params = params or ANALYSIS_PARAMS
    logger.info("Building mart: %s", spec.table_name)

    lf, min_batch_date = _prepare_and_concat_sources(spec, lf_dict)

    lf = _fix_adbc_numerics(lf)

    if spec.primary_key:
        lf = lf.unique(subset=spec.primary_key, keep="last")

    lf = _apply_joins(lf, spec.joins, lf_dict)

    lf = _fix_adbc_numerics(lf)

    active_aggregations = list(spec.aggregations) if spec.aggregations else []
    existing_agg_targets = {am.target for am in active_aggregations}

    if spec.window_aggregations:
        src_cols = lf.collect_schema().names()
        for win in spec.window_aggregations:
            if win.source_col not in existing_agg_targets and win.source_col not in (
                spec.group_by or []
            ):
                if win.source_col in src_cols:
                    logger.info(
                        "Dynamically adding missing window source column '%s' to active aggregations of '%s'",
                        win.source_col,
                        spec.table_name,
                    )
                    active_aggregations.append(
                        ColumnMapping(
                            target=win.source_col,
                            source_table=win.source_table,
                            source_col=win.source_col,
                            agg_func="first",
                            default=0.0,
                        )
                    )
                    existing_agg_targets.add(win.source_col)

    lf = _build_aggregation(lf, spec.group_by or [], active_aggregations)

    lf = _apply_window_aggregations(lf, spec.window_aggregations, params)

    if min_batch_date and spec.partition_column:
        lf = lf.filter(pl.col(spec.partition_column) >= min_batch_date)

    lf = _derive_columns(lf, spec.derived_columns)

    lf = _apply_output_schema(lf, spec)

    logger.info("Mart '%s' built successfully.", spec.table_name)
    return lf
