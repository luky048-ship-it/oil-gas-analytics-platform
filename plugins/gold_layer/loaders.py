# plugins/gold_layer/loaders.py
import logging
from datetime import date, datetime
from typing import List, Optional

import polars as pl
from gold_layer.connections import (get_postgres_uri, get_s3_fs,
                                    get_s3_storage_options)
from gold_layer.constants import SILVER_PREFIX

logger = logging.getLogger(__name__)


def load_silver_dataset(
    table_name: str, partition_dates: Optional[List[str]] = None
) -> pl.LazyFrame:
    """
    Создаёт ленивый граф вычислений (LazyFrame) над Parquet-файлами в S3.
    Использует нативные storage_options Polars для безопасного подключения к MinIO/S3
    (поддержка aws_allow_http). Автоматически извлекает Hive-партиции.
    """
    fs = get_s3_fs()
    s3_options = get_s3_storage_options()

    base_prefix = (
        SILVER_PREFIX if SILVER_PREFIX.startswith("s3://") else f"s3://{SILVER_PREFIX}"
    )
    base_path = f"{base_prefix}/{table_name}"

    if partition_dates:
        existing_paths = []
        for dt in partition_dates:
            glob_path = f"{base_path}/partition_date={dt}/*.parquet"
            if fs.glob(glob_path.replace("s3://", "")):
                existing_paths.append(glob_path)

        if not existing_paths:
            logger.warning(
                "No parquet files found for table '%s' in partitions: %s",
                table_name,
                partition_dates,
            )
            return pl.LazyFrame()

        logger.info(
            "Loading '%s' from %d selected partitions.", table_name, len(existing_paths)
        )
        target_path = existing_paths
    else:
        target_path = f"{base_path}/**/*.parquet"
        logger.info("Loading entire history for '%s'.", table_name)

    return pl.scan_parquet(
        target_path, storage_options=s3_options, hive_partitioning=True
    )


def discover_new_partitions(
    table_name: str, last_watermark: Optional[date]
) -> List[str]:
    """
    Сканирует иерархию директорий Silver-слоя для поиска новых партиций.
    Сравнивает извлечённые даты с last_watermark.
    """
    if isinstance(last_watermark, str):
        try:
            last_watermark = datetime.strptime(last_watermark, "%Y-%m-%d").date()
        except ValueError:
            logger.error("Invalid watermark string format: %s", last_watermark)
            last_watermark = None
    elif isinstance(last_watermark, datetime):
        last_watermark = last_watermark.date()

    fs = get_s3_fs()
    base_path = f"{SILVER_PREFIX.replace('s3://', '')}/{table_name}"

    try:
        partition_dirs = fs.ls(base_path)
    except FileNotFoundError:
        logger.error("Silver table '%s' not found at '%s'.", table_name, base_path)
        return []

    new_dates = []
    for d in partition_dirs:
        if "partition_date=" not in d:
            continue

        date_str = d.split("partition_date=")[-1]
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            if last_watermark is None or dt > last_watermark:
                new_dates.append(date_str)
        except ValueError:
            logger.debug("Skipping invalid partition directory format: %s", d)
            continue

    sorted_dates = sorted(new_dates)
    logger.info(
        "Discovered %d new partitions for '%s' (watermark: %s).",
        len(sorted_dates),
        table_name,
        last_watermark,
    )

    return sorted_dates


def load_gold_dataset(table_name: str, query: Optional[str] = None) -> pl.LazyFrame:
    """
    Синхронно вычитывает Gold-датасет из Postgres и переводит его в LazyFrame.

    Рекомендуется передавать параметр `query` для ограничения объёма (Pushdown),
    так как данные сначала полностью загружаются в RAM.
    """
    uri = get_postgres_uri()

    sql = query if query else f"SELECT * FROM {table_name}"

    if not query:
        logger.warning(
            "Loading entire Gold table '%s' without limits. "
            "Consider using a specific query for large tables to avoid OOM.",
            table_name,
        )

    try:
        try:
            df = pl.read_database(sql, connection=uri, engine="adbc")
        except (ImportError, RuntimeError):
            logger.warning(
                "ADBC not available, falling back to default Polars DB engine."
            )
            df = pl.read_database(sql, connection=uri)

        logger.info("Successfully loaded %d records from '%s'.", len(df), table_name)
        return df.lazy()

    except Exception as e:
        logger.error("Failed to load Gold dataset '%s': %s", table_name, e)
        raise RuntimeError(f"Database read error for {table_name}") from e
