from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import polars as pl

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def execute_dq_pipeline(
    dataset: str,
    partition_path: str,
    expected_schema: Dict[str, Any],
    key_columns: List[str],
    parent_joins: List[Dict[str, str]],
    business_rules_config: Dict[str, Any],
    historical_stats: Dict[str, Tuple[float, float]],
    execution_date: str,
    s3_options: dict,
) -> Tuple[List[Dict[str, Any]], pl.DataFrame, pl.DataFrame]:

    # 1. Lazy Load с передачей параметров fsspec через storage_options
    lf = pl.scan_parquet(partition_path, storage_options=s3_options)

    # Приведение типов согласно контракту (Schema Enforcement)
    lf = lf.cast(expected_schema)

    rule_exprs: Dict[str, pl.Expr] = {}

    # 2. Referential Integrity (Anti-Join) с проверкой на существование файлов
    import s3fs

    fs = s3fs.S3FileSystem(**s3_options)

    for join in parent_joins:
        child_key = join["child_key"]
        parent_path = join["parent_path"]

        # Проверяем, есть ли файлы по пути (чтобы scan_parquet не упал)
        if fs.glob(parent_path):
            parent_lf = (
                pl.scan_parquet(parent_path, storage_options=s3_options)
                .select([pl.col(join["parent_key"]).alias(child_key)])
                .unique()
                .with_columns(pl.lit(True).alias(f"__fk_{child_key}"))
            )
            lf = lf.join(parent_lf, on=child_key, how="left")
            rule_exprs[f"fk_{child_key}"] = pl.col(f"__fk_{child_key}").fill_null(False)
        else:
            logger.warning(
                f"Parent path {parent_path} is empty. RI will fail for all rows."
            )
            rule_exprs[f"fk_{child_key}"] = pl.lit(False)

    # 3. Business Rules
    for col in business_rules_config.get("not_null_columns", []):
        rule_exprs[f"not_null_{col}"] = pl.col(col).is_not_null()

    for col, (min_v, max_v) in business_rules_config.get("value_ranges", {}).items():
        cond = pl.lit(True)
        if min_v is not None:
            cond = cond & (pl.col(col) >= min_v)
        if max_v is not None:
            cond = cond & (pl.col(col) <= max_v)
        rule_exprs[f"range_{col}"] = pl.col(col).is_null() | cond

    # 4. Построение итогового флага (Single Pass Graph)
    validation_cols = []
    for name, expr in rule_exprs.items():
        col_name = f"__is_valid_{name}"
        lf = lf.with_columns(expr.alias(col_name))
        validation_cols.append(col_name)

    if validation_cols:
        lf = lf.with_columns(pl.all_horizontal(validation_cols).alias("__is_valid"))
    else:
        lf = lf.with_columns(pl.lit(True).alias("__is_valid"))

    # 5. Единственный Collect (Streaming Mode)
    df = lf.collect(streaming=True)

    results = []
    created_at = datetime.now(timezone.utc).isoformat()

    for name in rule_exprs.keys():
        col_name = f"__is_valid_{name}"
        failed_count = df.filter(~pl.col(col_name)).height
        results.append(
            {
                "dataset": dataset,
                "validation_type": f"Row-Level: {name}",
                "status": "FAIL" if failed_count > 0 else "PASS",
                "failed_rows": failed_count,
                "checked_rows": df.height,
                "message": (
                    f"Failed {failed_count} rows" if failed_count > 0 else "Passed"
                ),
                "created_at": created_at,
            }
        )

    internal_cols = [c for c in df.columns if c.startswith("__")]
    valid_df = df.filter(pl.col("__is_valid")).drop(internal_cols)
    invalid_df = df.filter(~pl.col("__is_valid")).drop(internal_cols)

    return results, valid_df, invalid_df
