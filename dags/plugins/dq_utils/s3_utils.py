# plugins/dq_utils/s3_utils.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import List

import s3fs
import pyarrow.parquet as pq
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def get_s3_storage_options(conn_id: str = "s3_default") -> dict:
    """
    Retrieves credentials from Airflow Connection and returns storage_options
    compatible with Polars and s3fs.
    """
    try:
        conn = BaseHook.get_connection(conn_id)

        options = {"key": conn.login, "secret": conn.password, "client_kwargs": {}}

        if conn.extra_dejson:
            endpoint_url = conn.extra_dejson.get(
                "endpoint_url"
            ) or conn.extra_dejson.get("host")
            if endpoint_url:
                options["client_kwargs"]["endpoint_url"] = endpoint_url

            # Allow insecure connections if explicitly requested in extra (e.g., local MinIO)
            if conn.extra_dejson.get("secure") is False:
                options["client_kwargs"]["use_ssl"] = False
                options["client_kwargs"]["verify"] = False

        return options
    except Exception as e:
        raise AirflowFailException(
            f"CRITICAL: Failed to retrieve S3 connection '{conn_id}': {e}"
        )


def discover_available_partitions(
    dataset: str,
    execution_date: str,
    s3_options: dict,
    base_path: str = "s3://datalake/raw",
) -> List[str]:
    """
    Checks for the existence of parquet partitions for the given dataset and date.
    Returns a list of discovered partition paths.
    Raises AirflowFailException on missing partitions to enforce SLA and pipeline integrity.
    """
    fs = s3fs.S3FileSystem(**s3_options)
    dataset_path = f"{base_path.rstrip('/')}/{dataset}"

    try:
        # Looking for Hive-style partitioned paths containing the execution date
        search_pattern = f"{dataset_path}/*{execution_date}*/*.parquet"
        files = fs.glob(search_pattern)

        # Fallback to direct file naming if not strictly Hive-partitioned
        if not files:
            search_pattern = f"{dataset_path}/*{execution_date}*.parquet"
            files = fs.glob(search_pattern)

    except Exception as e:
        raise AirflowFailException(
            f"CRITICAL: S3 listing failed for {dataset_path}: {e}"
        )

    if not files:
        raise AirflowFailException(
            f"CRITICAL: Missing partitions for dataset '{dataset}' on execution date '{execution_date}'"
        )

    # Ensure fully qualified S3 paths
    valid_paths = [f"s3://{p}" if not p.startswith("s3://") else p for p in files]
    logger.info(
        f"Discovered {len(valid_paths)} partitions for {dataset} on {execution_date}."
    )

    return valid_paths


def validate_file_integrity(
    dataset: str, partition_path: str, s3_options: dict
) -> DQResult:
    """
    Validates physical file integrity without loading the dataset into memory.
    Checks existence, readability, and ensures the parquet file is not empty.
    Raises AirflowFailException for critical corruptions.
    """
    fs = s3fs.S3FileSystem(**s3_options)

    try:
        if not fs.exists(partition_path):
            raise AirflowFailException(
                f"CRITICAL: File does not exist at {partition_path}"
            )

        with fs.open(partition_path, "rb") as f:
            pf = pq.ParquetFile(f)
            num_rows = pf.metadata.num_rows

            if num_rows == 0:
                raise AirflowFailException(
                    f"CRITICAL: Empty parquet file detected at {partition_path}"
                )

        return DQResult(
            dataset=dataset,
            validation_type="File Integrity Layer 1",
            status="PASS",
            failed_rows=0,
            checked_rows=num_rows,
            message="File is physically valid and readable",
            created_at=datetime.utcnow(),
        )

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"CRITICAL: Corrupted parquet file at {partition_path}. Error: {str(e)}"
        )
