# plugins/dq_utils/freshness_validator.py
from __future__ import annotations

import logging
from datetime import datetime
import s3fs

from airflow.exceptions import AirflowFailException

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_data_freshness(
    dataset: str,
    partition_date: str,
    max_delay_minutes: int,
    s3_options: dict,
    base_path: str = "s3://datalake/raw",
) -> DQResult:
    """
    Validates data freshness by checking the S3 object's LastModified timestamp
    against the expected SLA. Does not require loading the dataset.
    Raises AirflowFailException if SLA is breached.
    """
    fs = s3fs.S3FileSystem(**s3_options)
    dataset_path = f"{base_path.rstrip('/')}/{dataset}"

    search_pattern = f"{dataset_path}/*{partition_date}*/*.parquet"
    files = fs.glob(search_pattern)

    if not files:
        search_pattern = f"{dataset_path}/*{partition_date}*.parquet"
        files = fs.glob(search_pattern)

    if not files:
        raise AirflowFailException(
            f"CRITICAL: No files found for freshness validation: {dataset} @ {partition_date}"
        )

    # Get the latest modified file in the partition
    latest_modified = None
    for f in files:
        info = fs.info(f)
        last_modified = info.get("LastModified")
        if last_modified:
            # Ensure naive UTC datetime for comparison
            if last_modified.tzinfo:
                last_modified = last_modified.replace(tzinfo=None)
            if not latest_modified or last_modified > latest_modified:
                latest_modified = last_modified

    if not latest_modified:
        logger.warning(
            "Could not determine LastModified from S3 metadata. Skipping freshness check."
        )
        return DQResult(
            dataset, "Freshness SLA", "WARNING", 0, 0, "No metadata", datetime.utcnow()
        )

    now = datetime.utcnow()
    delay_minutes = (now - latest_modified).total_seconds() / 60.0

    if delay_minutes > max_delay_minutes:
        error_msg = f"Freshness SLA breach for '{dataset}'. Delay: {delay_minutes:.1f}m > Allowed: {max_delay_minutes}m."
        logger.error(error_msg)
        raise AirflowFailException(f"HIGH: {error_msg}")

    return DQResult(
        dataset=dataset,
        validation_type="Freshness SLA",
        status="PASS",
        failed_rows=0,
        checked_rows=len(files),
        message=f"Freshness met. Delay: {delay_minutes:.1f}m.",
        created_at=datetime.utcnow(),
    )
