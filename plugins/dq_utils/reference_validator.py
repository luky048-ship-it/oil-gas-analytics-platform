# plugins/dq_utils/reference_validator.py
from __future__ import annotations

import logging
from datetime import datetime

import polars as pl
from airflow.exceptions import AirflowFailException

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_reference_integrity(
    lf_child: pl.LazyFrame,
    lf_parent: pl.LazyFrame,
    child_key: str,
    parent_key: str,
    dataset: str,
) -> DQResult:
    """
    Validates referential integrity using a scalable anti-join.
    Identifies orphan records in the child dataset that lack a corresponding parent.
    Raises AirflowFailException if orphan records are found (CRITICAL severity).
    """
    # Perform a lazy anti-join to find orphans
    orphan_lf = lf_child.join(
        lf_parent.select([parent_key]),
        left_on=child_key,
        right_on=parent_key,
        how="anti",
    )

    orphan_count = orphan_lf.select(pl.len()).collect().item()
    total_rows = lf_child.select(pl.len()).collect().item()

    if orphan_count > 0:
        error_msg = f"Referential integrity violation in '{dataset}': Found {orphan_count} orphan records for key '{child_key}'."
        logger.error(error_msg)
        raise AirflowFailException(f"CRITICAL: {error_msg}")

    return DQResult(
        dataset=dataset,
        validation_type=f"Referential Integrity ({child_key})",
        status="PASS",
        failed_rows=0,
        checked_rows=total_rows,
        message="All foreign keys successfully resolved.",
        created_at=datetime.utcnow(),
    )
