# plugins/dq_utils/core.py
from __future__ import annotations

import logging
import typing
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import polars as pl
from airflow.exceptions import AirflowFailException
from core.s3_connection import get_polars_storage_options, get_s3_filesystem
from dq_utils.business_validator import (validate_business_rules,
                                         validate_duplicate_keys,
                                         validate_null_thresholds)
from dq_utils.statistical_validator import validate_distribution_drift

logger = logging.getLogger(__name__)


def execute_dq_pipeline(
    dataset: str,
    partition_path: str,
    expected_schema: Dict[str, pl.DataType],
    key_columns: List[str],
    parent_joins: List[Dict[str, str]],
    business_rules_config: Dict[str, Any],
    historical_stats: Dict[str, Tuple[float, float]],
    execution_date: str,
) -> Tuple[List[dict], pl.DataFrame, pl.DataFrame]:

    logger.info(f"Starting DQ pipeline for {dataset} (date: {execution_date})")

    if not partition_path:
        raise AirflowFailException(f"Partition path is empty for dataset {dataset}")

    try:
        polars_opts = get_polars_storage_options()
        fs = get_s3_filesystem()
    except Exception as e:
        logger.error(f"S3 Connection failed: {str(e)}")
        raise AirflowFailException(f"S3 Connection Error: {str(e)}")

    scan_path = partition_path
    if not partition_path.endswith(".parquet") and "*" not in partition_path:
        scan_path = f"{partition_path.rstrip('/')}/**/*.parquet"

    logger.info(f"Scanning path for DQ: {scan_path}")

    try:
        lf = pl.scan_parquet(
            scan_path, storage_options=polars_opts, hive_partitioning=True
        )

        available_cols = lf.collect_schema().names()
        target_cols = [col for col in expected_schema.keys() if col in available_cols]

        lf = lf.select(target_cols).cast(
            {col: expected_schema[col] for col in target_cols}
        )
    except Exception as e:
        raise AirflowFailException(
            f"Failed to scan or cast parquet at {scan_path}: {e}"
        )

    lf = _sanitize_datetime_columns(lf, expected_schema)

    results = _run_pre_materialization_checks(
        lf, dataset, key_columns, business_rules_config, historical_stats
    )

    lf, rule_exprs = _apply_row_level_and_fk_rules(
        lf, dataset, parent_joins, business_rules_config, fs, polars_opts
    )

    try:
        df = lf.collect(streaming=True)
    except Exception as e:
        logger.warning(
            f"Streaming engine failed, falling back to standard collect: {e}"
        )
        df = lf.collect()  # Fallback если Join не поддержал стриминг

    return _finalize_results(df, dataset, results, rule_exprs)


def _sanitize_datetime_columns(
    lf: pl.LazyFrame, schema: Dict[str, Any]
) -> pl.LazyFrame:

    datetime_exprs = []

    for col_name, dtype in schema.items():
        if isinstance(dtype, (pl.Datetime, pl.Date)):
            datetime_exprs.append(pl.col(col_name).cast(pl.Datetime("us")))
    if datetime_exprs:
        lf = lf.with_columns(datetime_exprs)

    return lf


def _run_pre_materialization_checks(
    lf: pl.LazyFrame, dataset: str, key_columns: List[str], config: Dict, hist: Dict
) -> List[dict]:
    results = validate_business_rules(lf, dataset)
    results.append(
        validate_null_thresholds(
            lf, dataset, {c: 0.0 for c in config.get("not_null_columns", [])}
        )
    )
    results.append(validate_duplicate_keys(lf, dataset, key_columns))

    if config.get("statistical_monitored_columns") and hist:
        results.extend(
            validate_distribution_drift(
                lf, dataset, config["statistical_monitored_columns"], hist
            )
        )

    return [r.__dict__ for r in results]


def _apply_row_level_and_fk_rules(
    lf: pl.LazyFrame,
    dataset: str,
    parent_joins: List[Dict],
    config: Dict,
    fs,
    polars_opts,
) -> Tuple[pl.LazyFrame, Dict[str, pl.Expr]]:

    rule_exprs = {}
    logger.debug(f"Applying row-level rules for dataset: {dataset}")

    for join in parent_joins:
        child_key, parent_path = join["child_key"], join["parent_path"]

        if not fs.glob(parent_path):
            raise AirflowFailException(
                f"Parent dataset {parent_path} missing for {dataset}"
            )

        parent_lf = (
            pl.scan_parquet(parent_path, storage_options=polars_opts)
            .select([pl.col(join["parent_key"]).alias(child_key)])
            .unique()
            .with_columns(pl.lit(True).alias(f"__fk_{child_key}"))
        )
        lf = lf.join(parent_lf, on=child_key, how="left")
        rule_exprs[f"fk_{child_key}"] = pl.col(f"__fk_{child_key}").fill_null(False)

    for col in config.get("not_null_columns", []):
        rule_exprs[f"not_null_{col}"] = pl.col(col).is_not_null()

    for col, (min_v, max_v) in config.get("value_ranges", {}).items():
        cond = pl.lit(True)
        if min_v is not None:
            cond &= pl.col(col) >= min_v
        if max_v is not None:
            cond &= pl.col(col) <= max_v
        rule_exprs[f"range_{col}"] = pl.col(col).is_null() | cond

    validation_cols = []
    for name, expr in rule_exprs.items():
        col_name = f"__is_valid_{name}"
        lf = lf.with_columns(expr.alias(col_name))
        validation_cols.append(col_name)

    lf = lf.with_columns(
        pl.all_horizontal(validation_cols).alias("__is_valid")
        if validation_cols
        else pl.lit(True)
    )

    return lf, rule_exprs


def _finalize_results(
    df: pl.DataFrame, dataset: str, results: List[dict], rule_exprs: Dict
) -> Tuple[List[dict], pl.DataFrame, pl.DataFrame]:
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
