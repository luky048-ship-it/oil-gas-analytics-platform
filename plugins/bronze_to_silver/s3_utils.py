# plugins/bronze_to_silver/s3_utils.py
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_S3_FORCE_PATH_STYLE"] = "true"
os.environ["AWS_S3_ALLOW_HTTP"] = "true"


def get_s3_storage_options(conn_id: str = "aws_default") -> Dict[str, Any]:
    try:
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        endpoint = extra.get("endpoint_url") or extra.get("host")

        return {
            "key": conn.login,
            "secret": conn.password,
            "aws_access_key_id": conn.login,
            "aws_secret_access_key": conn.password,
            "aws_region": "us-east-1",
            "aws_endpoint": endpoint,
            "aws_endpoint_url": endpoint,  # для s3fs
            "endpoint_url": endpoint,  # для s3fs (дубль)
            "aws_allow_http": "true",
            "aws_metadata_lookups": "false",
            "force_path_style": "true",
        }
    except Exception as e:
        logger.warning(f"Connection {conn_id} not found, using defaults.")
        return {
            "aws_access_key_id": "admin",
            "aws_secret_access_key": "password",
            "aws_endpoint": "http://minio:9000",
            "aws_endpoint_url": "http://minio:9000",
            "aws_allow_http": "true",
            "aws_region": "us-east-1",
        }


def load_bronze_dataset(
    dataset_paths: List[str],
    storage_options: Dict[str, Any],
    watermark: Optional[datetime] = None,
    time_column: Optional[str] = None,
) -> pl.LazyFrame:
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
