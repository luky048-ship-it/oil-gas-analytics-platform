from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import polars as pl

from dq_utils.dq_reporter import DQResult
from dq_utils.business_validator import (validate_null_thresholds,
                                        validate_duplicate_keys,
                                        validate_business_rules)
from dq_utils.reference_validator import validate_reference_integrity
# from dq_utils.statistical_validator import validate_distribution_drift, validate_volume_anomaly

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
    # Normalize s3_options for Polars
    pl_opts = {
        "key": s3_options.get("key"),
        "secret": s3_options.get("secret"),
        "endpoint_url": s3_options.get("client_kwargs", {}).get("endpoint_url")
    }

    lf = pl.scan_parquet(partition_path, storage_options=pl_opts)

    # Schema Enforcement and Timestamp Truncation
    lf = lf.cast(expected_schema)
    for col_name, dtype in expected_schema.items():
        if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
            lf = lf.with_columns(pl.col(col_name).dt.truncate("1s"))

    results = []

    # Layer 2: Schema (Implicit in cast)

    # Layer 3: Business Rules
    biz_results = validate_business_rules(lf, dataset)
    results.extend([r.__dict__ for r in biz_results])

    # Layer 7: Completeness (Nulls and Duplicates)
    # Using small sample or specific aggregations for DQResult objects
    null_res = validate_null_thresholds(lf, dataset, {c: 0.0 for c in business_rules_config.get("not_null_columns", [])})
    results.append(null_res.__dict__)

    dup_res = validate_duplicate_keys(lf, dataset, key_columns)
    results.append(dup_res.__dict__)

    # 4. Building the Validation Graph for valid/invalid split
    rule_exprs: Dict[str, pl.Expr] = {}

    for col in business_rules_config.get("not_null_columns", []):
        rule_exprs[f"not_null_{col}"] = pl.col(col).is_not_null()

    for col, (min_v, max_v) in business_rules_config.get("value_ranges", {}).items():
        cond = pl.lit(True)
        if min_v is not None:
            cond = cond & (pl.col(col) >= min_v)
        if max_v is not None:
            cond = cond & (pl.col(col) <= max_v)
        rule_exprs[f"range_{col}"] = pl.col(col).is_null() | cond

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

    internal_cols = [c for c in df.columns if c.startswith("__")]
    valid_df = df.filter(pl.col("__is_valid")).drop(internal_cols)
    invalid_df = df.filter(~pl.col("__is_valid")).drop(internal_cols)

    # Serialize datetimes for XCom
    for r in results:
        if isinstance(r.get("created_at"), datetime):
            r["created_at"] = r["created_at"].isoformat()

    return results, valid_df, invalid_df
