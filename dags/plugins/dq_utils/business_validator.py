# plugins/dq_utils/business_validator.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

import polars as pl

from dq_utils.dq_reporter import DQResult
from dq_utils.config import TABLE_CONTRACTS

logger = logging.getLogger(__name__)


def validate_null_thresholds(
    lf: pl.LazyFrame, dataset: str, thresholds: Dict[str, float]
) -> DQResult:
    """
    Validates the maximum allowed percentage of NULL values per column.
    Executes a single lazy aggregation query to prevent full dataset materialization.
    """
    total_rows_expr = pl.len().alias("total_rows")
    null_exprs = [
        pl.col(c).is_null().sum().alias(f"{c}_nulls") for c in thresholds.keys()
    ]

    agg_lf = lf.select([total_rows_expr] + null_exprs)
    result_df = agg_lf.collect()

    total_rows = result_df["total_rows"][0]
    if total_rows == 0:
        return DQResult(
            dataset,
            "Null Thresholds",
            "WARNING",
            0,
            0,
            "Empty dataset",
            datetime.utcnow(),
        )

    failed_checks = []
    for col, max_pct in thresholds.items():
        null_count = result_df[f"{col}_nulls"][0]
        actual_pct = (null_count / total_rows) * 100.0
        if actual_pct > max_pct:
            failed_checks.append(f"{col}: {actual_pct:.2f}% > {max_pct}%")

    status = "FAIL" if failed_checks else "PASS"
    message = (
        f"Null thresholds exceeded: {failed_checks}"
        if failed_checks
        else "All null thresholds met."
    )

    return DQResult(
        dataset=dataset,
        validation_type="Null Thresholds",
        status=status,
        failed_rows=0,  # Abstract metric for column-level checks
        checked_rows=total_rows,
        message=message,
        created_at=datetime.utcnow(),
    )


def validate_duplicate_keys(
    lf: pl.LazyFrame, dataset: str, key_columns: List[str]
) -> DQResult:
    """
    Validates uniqueness of primary/composite keys.
    Uses lazy groupby and aggregation to minimize memory footprint.
    """
    dup_count_df = (
        lf.group_by(key_columns)
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len().alias("duplicate_groups"))
        .collect()
    )

    total_rows = lf.select(pl.len()).collect().item()
    duplicate_groups = (
        dup_count_df["duplicate_groups"][0] if not dup_count_df.is_empty() else 0
    )

    status = "FAIL" if duplicate_groups > 0 else "PASS"
    message = (
        f"Found {duplicate_groups} duplicate key groups."
        if duplicate_groups > 0
        else "No duplicates found."
    )

    return DQResult(
        dataset=dataset,
        validation_type="Duplicate Keys",
        status=status,
        failed_rows=duplicate_groups,
        checked_rows=total_rows,
        message=message,
        created_at=datetime.utcnow(),
    )


def validate_business_rules(lf: pl.LazyFrame, dataset: str) -> List[DQResult]:
    """
    Validates domain-specific constraints (e.g., pressure > 0).
    Reads rules from TABLE_CONTRACTS and executes them via lazy aggregations.
    """
    contract = TABLE_CONTRACTS.get(dataset)
    if not contract:
        logger.warning(
            f"No contract found for dataset '{dataset}'. Skipping business rules."
        )
        return []

    results = []
    total_rows = lf.select(pl.len()).collect().item()

    if total_rows == 0:
        return []

    # 1. Validate Value Ranges
    for col, (min_val, max_val) in contract.value_ranges.items():
        exprs = []
        if min_val is not None:
            exprs.append(pl.col(col) < min_val)
        if max_val is not None:
            exprs.append(pl.col(col) > max_val)

        if exprs:
            combined_expr = pl.any_horizontal(exprs)
            failed_count = lf.filter(combined_expr).select(pl.len()).collect().item()

            status = "FAIL" if failed_count > 0 else "PASS"
            msg = f"Column {col} out of range ({min_val}, {max_val})."
            results.append(
                DQResult(
                    dataset,
                    f"Range Check: {col}",
                    status,
                    failed_count,
                    total_rows,
                    msg,
                    datetime.utcnow(),
                )
            )

    # 2. Validate Enums
    for col, allowed_values in contract.enums.items():
        failed_count = (
            lf.filter(~pl.col(col).is_in(allowed_values))
            .select(pl.len())
            .collect()
            .item()
        )
        status = "FAIL" if failed_count > 0 else "PASS"
        msg = f"Column {col} contains unapproved values."
        results.append(
            DQResult(
                dataset,
                f"Enum Check: {col}",
                status,
                failed_count,
                total_rows,
                msg,
                datetime.utcnow(),
            )
        )

    return results
