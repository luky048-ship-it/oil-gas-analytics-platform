# /plugins/bronze_to_silver/s3_utils.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def get_s3_storage_options(conn_id: str = "aws_default") -> Dict[str, Any]:
    """
    Извлекает креды S3 из Airflow Connection (совместимо с 2.10.4).
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson

        options = {
            "aws_access_key_id": conn.login,
            "aws_secret_access_key": conn.password,
        }

        if extra.get("endpoint_url"):
            options["endpoint_url"] = extra["endpoint_url"]
        if extra.get("region_name"):
            options["region"] = extra["region_name"]

        return options
    except Exception as e:
        logger.warning(
            f"Connection {conn_id} not found: {e}. Using local MinIO defaults."
        )
        return {
            "aws_access_key_id": "admin",
            "aws_secret_access_key": "password",
            "endpoint_url": "http://minio:9000",
            "region": "us-east-1",
        }


def load_bronze_dataset(
    dataset_paths: List[str],
    storage_options: Dict[str, Any],
    watermark: Optional[datetime] = None,
    time_column: Optional[str] = None,
) -> pl.LazyFrame:
    """
    Лениво загружает parquet-файлы из Bronze слоя.
    """
    if not dataset_paths:
        return pl.LazyFrame()

    pl_options = {
        "aws_access_key_id": storage_options.get("aws_access_key_id"),
        "aws_secret_access_key": storage_options.get("aws_secret_access_key"),
        "aws_allow_http": True,
    }

    if "endpoint_url" in storage_options:
        pl_options["endpoint_url"] = storage_options["endpoint_url"]
    if "region" in storage_options:
        pl_options["region"] = storage_options["region"]

    lf = pl.scan_parquet(
        source=dataset_paths,
        storage_options=pl_options,
        hive_partitioning=True,
    )

    if watermark is not None and time_column:
        lf = lf.filter(pl.col(time_column) >= watermark)

    return lf
