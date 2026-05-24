# gold_layer/generic_builder.py

"""
Универсальный конфигурационный движок построения витрин на Polars Lazy API.
Порядок графа: Concat -> Deduplication -> Join -> Aggregation -> Window -> Batch filter -> Derived -> Output.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import polars as pl
from gold_layer.config import (ANALYSIS_PARAMS, ColumnMapping, DerivedColumn,
                               JoinSpec, MartSpec, WindowAggregation)

logger = logging.getLogger(__name__)


def _get_max_window_days(spec: MartSpec) -> int:
    max_days = 0
    for win in spec.window_aggregations:
        win_val = ANALYSIS_PARAMS.get(win.window_expr, 0)
        # Значения > 1000 трактуем как минуты (например, 7*24*60 для минут)
        days = win_val / (24 * 60) if win_val > 1000 else win_val
        max_days = max(max_days, int(days))
    return max_days


def _prepare_and_concat_sources(
    spec: MartSpec,
    lf_dict: Dict[str, pl.LazyFrame],
) -> tuple[pl.LazyFrame, Optional[date]]:
    """
    Объединяет базовые таблицы (batch + history), исключая измерения (они джойнятся позже).
    Возвращает общий LazyFrame и минимальную дату батча (для последующего отсечения истории).
    """
    joined_tables = {j.right_table for j in spec.joins}
    base_tables = [t for t in spec.source_tables if t not in joined_tables]

    if not base_tables:
        raise ValueError(f"No base tables found for mart '{spec.table_name}'")

    batch_lfs = []
    hist_lfs = []
    min_batch_date = None

    for t in base_tables:
        if t not in lf_dict:
            logger.warning("Base table '%s' not found, skipping", t)
            continue

        lf = lf_dict[t]
        schema = lf.collect_schema().names()

        # Унификация временных колонок: если есть timestamp, но нет date
        if "date" not in schema and "timestamp" in schema:
            lf = lf.with_columns(pl.col("timestamp").dt.date().alias("date"))
            schema.append("date")

        # Разделение на batch и историю по имени таблицы
        if "history" in t.lower():
            hist_lfs.append((t, lf))
        else:
            batch_lfs.append(lf)
            if "date" in schema:
                # Безопасное ленивое извлечение минимальной даты
                t_min = lf.select(pl.col("date").min()).collect().item()
                if t_min:
                    if not min_batch_date or t_min < min_batch_date:
                        min_batch_date = t_min

    if not batch_lfs:
        raise RuntimeError("No active batch data provided for mart.")

    # Предикатный Pushdown для исторических данных
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

    # Диагональное объединение для защиты от несоответствия схем (Polars 1.4+)
    combined = pl.concat(batch_lfs, how="diagonal_relaxed")
    return combined, min_batch_date


def _apply_joins(
    lf: pl.LazyFrame, joins: List[JoinSpec], lf_dict: Dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    """Безопасные джойны ДО агрегации с кастингом ключей к String."""
    for i, js in enumerate(joins):
        right_lf = lf_dict.get(js.right_table)
        if right_lf is None:
            logger.warning(
                "Join #%d skipped: right table '%s' not found", i, js.right_table
            )
            continue

        logger.info("Applying join #%d: right=%s, how=%s", i, js.right_table, js.how)

        # Кастинг ключей для предотвращения silent type mismatch
        left_keys = [f"_jL_{c}" for c in js.left_on]
        right_keys = [f"_jR_{c}" for c in js.right_on]

        for col, new_col in zip(js.left_on, left_keys):
            lf = lf.with_columns(pl.col(col).cast(pl.String).alias(new_col))
        for col, new_col in zip(js.right_on, right_keys):
            right_lf = right_lf.with_columns(pl.col(col).cast(pl.String).alias(new_col))

        lf = lf.join(right_lf, left_on=left_keys, right_on=right_keys, how=js.how)
        lf = lf.drop(left_keys + right_keys)

    return lf


def _build_aggregation(
    lf: pl.LazyFrame, group_by: List[str], aggregations: List[ColumnMapping]
) -> pl.LazyFrame:
    """Агрегация. Колонки из JOIN, не указанные здесь, будут удалены."""
    if not group_by or not aggregations:
        return lf

    logger.info("Aggregation: group_by=%s, metrics=%d", group_by, len(aggregations))

    agg_exprs = []
    for cm in aggregations:
        func = cm.agg_func or "first"

        # null-safe aggregation (игнорируем NULL, если это не first)
        col_expr = (
            pl.col(cm.source_col).drop_nulls()
            if func != "first"
            else pl.col(cm.source_col)
        )

        if func == "zscore":
            raise ValueError("zscore is only supported in window_aggregations")

        expr = col_expr.first() if func == "first" else getattr(col_expr, func)()

        if cm.default is not None:
            expr = expr.fill_null(cm.default)

        agg_exprs.append(expr.alias(cm.target))

    return lf.group_by(group_by).agg(agg_exprs)


def _apply_window_aggregations(
    lf: pl.LazyFrame, window_aggs: List[WindowAggregation], params: Dict[str, Any]
) -> pl.LazyFrame:
    """Оконные функции. В Polars 1.4+ window_size строго int."""
    if not window_aggs:
        return lf

    sort_keys = list(
        {pb for win in window_aggs for pb in win.partition_by}
        | {win.order_by for win in window_aggs}
    )
    if sort_keys:
        lf = lf.sort(sort_keys)

    window_exprs = []
    for win in window_aggs:
        # Извлекаем и строго приводим к INT
        raw_size = params.get(win.window_expr, win.window_expr)
        window_size = int(raw_size)

        col_expr = pl.col(win.source_col)

        if win.agg_func in ("mean", "sum", "max", "min"):
            expr = getattr(col_expr, f"rolling_{win.agg_func}")(window_size=window_size)
        elif win.agg_func == "zscore":
            mean_expr = col_expr.rolling_mean(window_size=window_size)
            std_expr = col_expr.rolling_std(window_size=window_size).fill_null(0.0)

            # Защита от деления на 0
            expr = (
                pl.when(std_expr > 0.0)
                .then((col_expr - mean_expr) / std_expr)
                .otherwise(0.0)
            )
        else:
            raise ValueError(f"Unsupported window aggregation: {win.agg_func}")

        expr = expr.over(win.partition_by).alias(win.target)
        window_exprs.append(expr)

    return lf.with_columns(window_exprs)


def _derive_columns(lf: pl.LazyFrame, derivations: List[DerivedColumn]) -> pl.LazyFrame:
    """Бизнес-правила и производные формулы."""
    if not derivations:
        return lf

    safe_env = {"pl": pl, "ANALYSIS_PARAMS": ANALYSIS_PARAMS}
    exprs = []

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

        exprs.append(expr.alias(dc.target))

    return lf.with_columns(exprs)


def _apply_output_schema(lf: pl.LazyFrame, spec: MartSpec) -> pl.LazyFrame:
    """Контроль качества схемы и генерация суррогатных ключей."""
    output_schema = spec.output_schema
    current_cols = set(lf.collect_schema().names())

    if "load_timestamp" in output_schema and "load_timestamp" not in current_cols:
        lf = lf.with_columns(pl.lit(datetime.now(timezone.utc)).alias("load_timestamp"))
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

    # Fail-Fast проверки и автогенерация ID
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
                lf = lf.with_columns(pl.int_range(0, pl.len(), dtype=dtype).alias(col))
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

    # 1. Подготовка базовых данных (батч + история)
    lf, min_batch_date = _prepare_and_concat_sources(spec, lf_dict)

    # 2. Строгая дедупликация по полному primary_key
    if spec.primary_key:
        lf = lf.unique(subset=spec.primary_key, keep="last")

    # 3. Джойны (обогащение измерениями) – ДО агрегации
    lf = _apply_joins(lf, spec.joins, lf_dict)

    # 4. Агрегация
    lf = _build_aggregation(lf, spec.group_by or [], spec.aggregations)

    # 5. Оконные функции
    lf = _apply_window_aggregations(lf, spec.window_aggregations, params)

    # 6. Отсечение истории – оставляем только строки текущего батча
    if min_batch_date and spec.partition_column:
        lf = lf.filter(pl.col(spec.partition_column) >= min_batch_date)

    # 7. Производные колонки
    lf = _derive_columns(lf, spec.derived_columns)

    # 8. Финальная схема
    lf = _apply_output_schema(lf, spec)

    logger.info("Mart '%s' built successfully.", spec.table_name)
    return lf
