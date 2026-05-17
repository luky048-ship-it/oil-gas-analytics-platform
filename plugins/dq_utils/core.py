from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import polars as pl

from dq_utils.business_validator import (validate_business_rules,
                                         validate_duplicate_keys,
                                         validate_null_thresholds)
from dq_utils.dq_reporter import DQResult
from dq_utils.statistical_validator import validate_distribution_drift

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

    # 1. Lazy Load
    lf = pl.scan_parquet(partition_path, storage_options=s3_options)

    # Приведение типов согласно контракту (Schema Enforcement)
    lf = lf.cast(expected_schema)

    # Standardize timestamps: truncate to seconds
    for col_name, dtype in expected_schema.items():
        if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
            lf = lf.with_columns(pl.col(col_name).dt.truncate("1s"))

    results_obj = []

    # Layer 3: Business Rules
    results_obj.extend(validate_business_rules(lf, dataset))

    # Layer 7: Completeness (Nulls and Duplicates)
    # We pass 0.0 threshold for mandatory columns to detect ANY nulls
    results_obj.append(
        validate_null_thresholds(
            lf,
            dataset,
            {c: 0.0 for c in business_rules_config.get("not_null_columns", [])},
        )
    )
    results_obj.append(validate_duplicate_keys(lf, dataset, key_columns))

    # Layer 6: Statistical Drift (if stats provided)
    stats_cols = business_rules_config.get("statistical_monitored_columns", [])
    if stats_cols and historical_stats:
        results_obj.extend(
            validate_distribution_drift(lf, dataset, stats_cols, historical_stats)
        )

    rule_exprs: Dict[str, pl.Expr] = {}

    # 2. Referential Integrity (Anti-Join) with Flag logic for splitting
    import s3fs

    fs = s3fs.S3FileSystem(**s3_options)

    for join in parent_joins:
        child_key = join["child_key"]
        parent_path = join["parent_path"]

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
            rule_exprs[f"fk_{child_key}"] = pl.lit(False)

    # Row-level Business Rules for splitting valid/invalid
    for col in business_rules_config.get("not_null_columns", []):
        rule_exprs[f"not_null_{col}"] = pl.col(col).is_not_null()

    for col, range_val in business_rules_config.get("value_ranges", {}).items():
        min_v, max_v = range_val
        cond = pl.lit(True)
        if min_v is not None:
            cond = cond & (pl.col(col) >= min_v)
        if max_v is not None:
            cond = cond & (pl.col(col) <= max_v)
        rule_exprs[f"range_{col}"] = pl.col(col).is_null() | cond

    # 4. Final Flag system for atomic materialization
    validation_cols = []
    for name, expr in rule_exprs.items():
        col_name = f"__is_valid_{name}"
        lf = lf.with_columns(expr.alias(col_name))
        validation_cols.append(col_name)

    if validation_cols:
        lf = lf.with_columns(pl.all_horizontal(validation_cols).alias("__is_valid"))
    else:
        lf = lf.with_columns(pl.lit(True).alias("__is_valid"))

    # 5. Single Pass Collect
    df = lf.collect(streaming=True)

    created_at = datetime.now(timezone.utc).isoformat()

    # Convert results objects to dicts
    final_results = [r.__dict__ for r in results_obj]

    # Process Row-Level results from flags
    for name in rule_exprs.keys():
        col_name = f"__is_valid_{name}"
        failed_count = df.filter(~pl.col(col_name)).height
        final_results.append(
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

    # Ensure all created_at are serialized
    for r in final_results:
        if isinstance(r.get("created_at"), datetime):
            r["created_at"] = r["created_at"].isoformat()

    internal_cols = [c for c in df.columns if c.startswith("__")]
    valid_df = df.filter(pl.col("__is_valid")).drop(internal_cols)
    invalid_df = df.filter(~pl.col("__is_valid")).drop(internal_cols)

    return final_results, valid_df, invalid_df
