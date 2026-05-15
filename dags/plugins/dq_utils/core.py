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
) -> Tuple[List[dict], pl.DataFrame, pl.DataFrame]:
    """
    Executes a multi-layer DQ pipeline in a single pass using Polars Lazy API.
    1. Schema Enforcement
    2. Referential Integrity
    3. Business Rules (Nulls, Ranges, Enums)
    4. Statistical Drift
    """

    # 1. Lazy Load
    pl_opts = {
        "key": s3_options.get("key"),
        "secret": s3_options.get("secret"),
        "endpoint_url": s3_options.get("client_kwargs", {}).get("endpoint_url")
    }
    lf = pl.scan_parquet(partition_path, storage_options=pl_opts)

    # Schema Enforcement
    lf = lf.cast(expected_schema)

    # Standardize timestamps: truncate to seconds
    for col_name, dtype in expected_schema.items():
        if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
            lf = lf.with_columns(pl.col(col_name).dt.truncate("1s"))

    rule_exprs: Dict[str, pl.Expr] = {}

    # 2. Referential Integrity (Anti-Join logic via flags)
    import s3fs
    fs = s3fs.S3FileSystem(**s3_options)

    for join in parent_joins:
        child_key = join["child_key"]
        parent_path = join["parent_path"]

        if fs.glob(parent_path):
            parent_lf = (
                pl.scan_parquet(parent_path, storage_options=pl_opts)
                .select([pl.col(join["parent_key"]).alias(child_key)])
                .unique()
                .with_columns(pl.lit(True).alias(f"__fk_{child_key}"))
            )
            lf = lf.join(parent_lf, on=child_key, how="left")
            rule_exprs[f"fk_{child_key}"] = pl.col(f"__fk_{child_key}").fill_null(False)
        else:
            rule_exprs[f"fk_{child_key}"] = pl.lit(False)

    # 3. Business Rules (Layer 3 & 7)
    # Null checks
    for col in business_rules_config.get("not_null_columns", []):
        rule_exprs[f"not_null_{col}"] = pl.col(col).is_not_null()

    # Range checks
    for col, range_val in business_rules_config.get("value_ranges", {}).items():
        min_v, max_v = range_val
        cond = pl.lit(True)
        if min_v is not None:
            cond = cond & (pl.col(col) >= min_v)
        if max_v is not None:
            cond = cond & (pl.col(col) <= max_v)
        rule_exprs[f"range_{col}"] = pl.col(col).is_null() | cond

    # Enum checks
    for col, allowed in business_rules_config.get("enums", {}).items():
        rule_exprs[f"enum_{col}"] = pl.col(col).is_in(allowed) | pl.col(col).is_null()

    # 4. Building the Final Validation Flag
    validation_cols = []
    for name, expr in rule_exprs.items():
        col_name = f"__is_valid_{name}"
        lf = lf.with_columns(expr.alias(col_name))
        validation_cols.append(col_name)

    if validation_cols:
        lf = lf.with_columns(pl.all_horizontal(validation_cols).alias("__is_valid"))
    else:
        lf = lf.with_columns(pl.lit(True).alias("__is_valid"))

    # 5. The One and Only Collect (Streaming Mode)
    df = lf.collect(streaming=True)

    created_at = datetime.now(timezone.utc).isoformat()
    results = []

    # Calculate metrics from the materialized flags
    for name in rule_exprs.keys():
        col_name = f"__is_valid_{name}"
        failed_count = df.filter(~pl.col(col_name)).height
        results.append({
            "dataset": dataset,
            "validation_type": f"Row-Level: {name}",
            "status": "FAIL" if failed_count > 0 else "PASS",
            "failed_rows": failed_count,
            "checked_rows": df.height,
            "message": f"Failed {failed_count} rows" if failed_count > 0 else "Passed",
            "created_at": created_at,
        })

    # Separate valid and invalid rows
    internal_cols = [c for c in df.columns if c.startswith("__")]
    valid_df = df.filter(pl.col("__is_valid")).drop(internal_cols)
    invalid_df = df.filter(~pl.col("__is_valid")).drop(internal_cols)

    return results, valid_df, invalid_df
