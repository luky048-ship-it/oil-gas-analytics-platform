# plugins/dq_utils/schema_validator.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict

import polars as pl
from airflow.exceptions import AirflowFailException

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_schema_contract(
    lf: pl.LazyFrame, expected_schema: Dict[str, pl.DataType], dataset: str
) -> DQResult:
    """
    Validates the physical schema of the dataset against the contract.
    Checks for missing columns, unexpected columns, and datatype mismatches.
    Raises AirflowFailException on schema drift to prevent pipeline corruption.
    Uses LazyFrame.schema to avoid any data materialization.
    """
    actual_schema = lf.schema

    missing_columns = []
    type_mismatches = []
    unexpected_columns = []

    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            missing_columns.append(col_name)
        else:
            actual_type = actual_schema[col_name]
            # Exact match or compatible numeric/datetime casting can be considered here.
            # For strict enterprise contracts, we enforce exact logical type matches.
            if actual_type != expected_type:
                type_mismatches.append(
                    f"{col_name} (expected {expected_type}, got {actual_type})"
                )

    for col_name in actual_schema.keys():
        if col_name not in expected_schema:
            # Depending on policy, unexpected columns might be allowed (Add nullable column = YES)
            # but we log them for observability.
            unexpected_columns.append(col_name)

    if missing_columns or type_mismatches:
        error_msg = (
            f"Schema contract violation for '{dataset}'. "
            f"Missing: {missing_columns}. Mismatches: {type_mismatches}."
        )
        logger.error(error_msg)
        raise AirflowFailException(f"CRITICAL: {error_msg}")

    if unexpected_columns:
        logger.warning(
            f"Unexpected columns found in '{dataset}' (allowed by evolution policy): {unexpected_columns}"
        )

    return DQResult(
        dataset=dataset,
        validation_type="Schema Contract Validation",
        status="PASS",
        failed_rows=0,
        checked_rows=0,  # Metadata-only check
        message="Schema matches contract successfully.",
        created_at=datetime.utcnow(),
    )
