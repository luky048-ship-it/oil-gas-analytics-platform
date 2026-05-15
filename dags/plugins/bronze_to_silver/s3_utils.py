# plugins/bronze_to_silver/s3_utils.py
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from airflow.hooks.base import BaseHook


def get_s3_storage_options(conn_id: str = "s3_datalake") -> Dict[str, Any]:
    """
    Fetches S3 credentials from Airflow Connection and formats them
    for Polars and PyArrow storage_options.
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson

    options = {
        "aws_access_key_id": conn.login,
        "aws_secret_access_key": conn.password,
    }

    if extra.get("endpoint_url"):
        options["aws_endpoint_url"] = extra.get("endpoint_url")
    if extra.get("region_name"):
        options["aws_region"] = extra.get("region_name")

    return options


def load_bronze_dataset(
    dataset_paths: List[str],
    storage_options: Dict[str, Any],
    watermark: Optional[datetime] = None,
    time_column: Optional[str] = None,
) -> pl.LazyFrame:
    """
    Lazily loads parquet files from Bronze layer.
    Applies automatic predicate pushdown for the event time column based on watermark.
    """
    if not dataset_paths:
        return pl.LazyFrame()

    lf = pl.scan_parquet(
        dataset_paths,
        storage_options=storage_options,
        hive_partitioning=True,
    )

    if watermark and time_column:
        lf = lf.filter(pl.col(time_column) >= watermark)

    return lf
