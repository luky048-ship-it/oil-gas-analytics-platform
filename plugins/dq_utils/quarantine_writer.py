# plugins/dq_utils/quarantine_writer.py
from __future__ import annotations

import logging
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)


def write_quarantine_dataset(
    invalid_df: pl.DataFrame,
    dataset: str,
    validation_name: str,
    partition_date: str,
    s3_options: dict,
    base_path: str = "s3://datalake/quarantine",
) -> Optional[str]:
    """
    Writes invalid records to the Quarantine Zone in S3.
    Enforces isolation of bad data and appends validation metadata for auditing.
    Idempotent operation: overwrites the specific partition file for the run.

    Path format: s3://datalake/quarantine/{dataset}/partition_date=YYYY-MM-DD/
    """
    if invalid_df.height == 0:
        logger.info(
            f"No invalid records to quarantine for dataset '{dataset}' on {partition_date}."
        )
        return None

    # Append mandatory quarantine metadata columns
    quarantine_df = invalid_df.with_columns(
        [
            pl.lit("validation_failed").alias("__reason_code"),
            pl.lit(dataset).alias("__source_dataset"),
            pl.lit(validation_name).alias("__validation_name"),
            pl.lit(partition_date).alias("__execution_date"),
        ]
    )

    # Construct the target S3 path ensuring the required partition structure
    target_dir = f"{base_path.rstrip('/')}/{dataset}/partition_date={partition_date}"
    target_path = (
        f"{target_dir}/quarantined_{validation_name.replace(' ', '_').lower()}.parquet"
    )

    try:
        # Write to S3 using Polars native write_parquet with storage_options
        # This inherently handles overwriting the specific file, ensuring idempotency
        quarantine_df.write_parquet(
            target_path,
            compression="snappy",
            use_pyarrow=True,
            pyarrow_options={"storage_options": s3_options},
        )

        logger.warning(
            f"Quarantined {quarantine_df.height} invalid records for '{dataset}'. "
            f"Validation: '{validation_name}'. Path: {target_path}"
        )

        return target_path

    except Exception as e:
        error_msg = f"CRITICAL: Failed to write quarantine dataset to {target_path}. Error: {str(e)}"
        logger.error(error_msg)
        # We raise the exception because silently dropping bad data is strictly forbidden
        raise RuntimeError(error_msg) from e
