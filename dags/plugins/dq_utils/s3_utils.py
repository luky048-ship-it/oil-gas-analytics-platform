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

            if conn.extra_dejson.get("secure") is False:
                options["client_kwargs"]["use_ssl"] = False
                options["client_kwargs"]["verify"] = False

        return options
    except Exception as e:
        # Fallback for local testing if connection is not found
        logger.warning(f"Failed to retrieve S3 connection '{conn_id}': {e}. Using defaults.")
        return {
            "key": "admin",
            "secret": "password",
            "client_kwargs": {"endpoint_url": "http://minio:9000"}
        }


def discover_available_partitions(
    dataset: str,
    execution_date: str,
    s3_options: dict,
    base_path: str = "s3://datalake/raw",
) -> List[str]:
    """
    Checks for the existence of parquet partitions for the given dataset and date.
    Returns a list of discovered partition paths.
    """
    fs = s3fs.S3FileSystem(**s3_options)
    dataset_path = f"{base_path.rstrip('/')}/{dataset}"

    try:
        # Expected: datalake/raw/wells/ (non-fact)
        # Expected: datalake/raw/production/partition_date=2024-01-01/ (fact)

        partition_path = f"{dataset_path}/partition_date={execution_date}"
        if fs.exists(partition_path):
             files = fs.glob(f"{partition_path}/*.parquet")
        else:
             # Try non-partitioned
             files = fs.glob(f"{dataset_path}/*.parquet")

    except Exception as e:
        raise AirflowFailException(
            f"CRITICAL: S3 listing failed for {dataset_path}: {e}"
        )

    if not files:
        logger.warning(f"No partitions found for dataset '{dataset}' on date '{execution_date}'")
        return []

    valid_paths = [f"s3://{p}" if not p.startswith("s3://") else p for p in files]
    return valid_paths


def validate_file_integrity(
    dataset: str, partition_path: str, s3_options: dict
) -> DQResult:
    """
    Validates physical file integrity.
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
