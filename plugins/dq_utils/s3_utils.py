# plugins/dq_utils/s3_utils.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, cast

import pyarrow.parquet as pq
import s3fs
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def get_s3fs_client(s3_options: dict) -> s3fs.S3FileSystem:
    """Безопасно создает клиент s3fs, отсекая ключи Polars."""
    s3fs_kwargs = {
        "key": s3_options.get("key"),
        "secret": s3_options.get("secret"),
        "endpoint_url": s3_options.get("endpoint_url"),
    }
    if "endpoint_url" in s3_options:
        s3fs_kwargs["client_kwargs"] = {"endpoint_url": s3_options["endpoint_url"]}

    if "use_ssl" in s3_options:
        s3fs_kwargs["use_ssl"] = s3_options["use_ssl"]

    s3fs_kwargs = {k: v for k, v in s3fs_kwargs.items() if v is not None}
    return s3fs.S3FileSystem(**s3fs_kwargs)


def get_s3_storage_options(conn_id: str = "aws_default") -> dict[str, Any]:
    """
    Retrieves credentials from Airflow Connection and returns a FLAT storage_options
    dictionary compatible with BOTH Polars (Rust native) and s3fs (Python).
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        endpoint = extra.get("endpoint_url") or extra.get("host")

        options: dict[str, Any] = {
            "key": conn.login,
            "secret": conn.password,
            "aws_access_key_id": conn.login,
            "aws_secret_access_key": conn.password,
            "aws_region": "us-east-1",
        }

        if endpoint:
            options["endpoint_url"] = endpoint  # для s3fs
            options["aws_endpoint"] = endpoint  # для Polars Rust

            if endpoint.startswith("http://"):
                options["aws_allow_http"] = "true"
                options["aws_region"] = "us-east-1"

        secure = extra.get("secure")
        if secure is not None and str(secure).lower() in ("false", "0", "no"):
            options["use_ssl"] = False
            options["verify"] = False
            options["aws_allow_http"] = "true"

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
    fs = get_s3fs_client(s3_options)
    bucket_path = base_path.replace("s3://", "").rstrip("/")
    dataset_path = f"{bucket_path}/{dataset}"

    try:
        search_pattern = f"{dataset_path}/*{execution_date}*/*.parquet"
        files = fs.glob(search_pattern)

        if not files:
            search_pattern = f"{dataset_path}/*{execution_date}*.parquet"
            files = fs.glob(search_pattern)
    except Exception as e:
        logger.error(f"S3 Listing failed: {str(e)}")
        raise AirflowFailException(f"CRITICAL: S3 Listing failed: {str(e)}")

    return [
        f"s3://{str(p)}" if not str(p).startswith("s3://") else str(p)
        for p in files
        if isinstance(p, (str, bytes))
    ]


def validate_file_integrity(
    dataset: str, partition_path: str, s3_options: dict
) -> DQResult:
    fs = get_s3fs_client(s3_options)

    try:
        if not fs.exists(partition_path):
            raise AirflowFailException(
                f"CRITICAL: File does not exist at {partition_path}"
            )

        with fs.open(partition_path, "rb") as f:
            pf = pq.ParquetFile(f)
            num_rows = pf.metadata.num_rows

            if num_rows == 0:
                logger.warning(
                    f"No partitions found for dataset '{dataset}' on date. Skipping."
                )
                return DQResult(
                    dataset=dataset,
                    validation_type="File Integrity Layer 1",
                    status="FAIL",
                    failed_rows=0,
                    checked_rows=0,
                    message=f"File is empty: {partition_path}",
                    created_at=datetime.utcnow(),
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
