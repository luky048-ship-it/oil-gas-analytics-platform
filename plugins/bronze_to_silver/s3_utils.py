# plugins/bronze_to_silver/s3_utils.py
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import polars as pl
from core.s3_connection import get_polars_storage_options

logger = logging.getLogger(__name__)


def get_s3_storage_options(conn_id: str = "aws_default") -> Dict[str, Any]:
    """
    Получает настройки подключения для Polars.
    """
    logger.debug(f"Fetching S3 storage options for connection: {conn_id}")
    return get_polars_storage_options(conn_id)


def load_bronze_dataset(
    dataset_paths: List[str],
    storage_options: Dict[str, Any],
    watermark: Optional[datetime] = None,
    time_column: Optional[str] = None,
) -> pl.LazyFrame:
    """
    Загружает Bronze dataset с обработкой ошибок и логированием.
    """
    if not dataset_paths:
        logger.warning("load_bronze_dataset called with empty dataset_paths list.")
        return pl.LazyFrame()

    final_paths = []
    for p in dataset_paths:
        clean_p = p.rstrip("/").replace("/*.parquet", "")
        final_paths.append(f"{clean_p}/*.parquet")

    logger.info(
        f"Attempting to scan {len(dataset_paths)} path(s). "
        f"Watermark: {watermark}, Time Column: {time_column}"
    )

    try:
        lf = pl.scan_parquet(
            final_paths,
            storage_options=storage_options,
            hive_partitioning=True,
        )

        if watermark and time_column:
            logger.info(f"Applying watermark filter: {time_column} >= {watermark}")
            # Приводим watermark к типу колонки для корректного сравнения
            column_schema = lf.collect_schema()
            if time_column in column_schema:
                column_type = column_schema[time_column]
                # Если колонка имеет тип Date, приводим datetime к date
                if column_type == pl.Date:
                    filter_val = watermark.date() if isinstance(watermark, datetime) else watermark
                elif column_type.base_type() == pl.Datetime:
                    # Handle timezone mismatch between tz-aware watermark and tz-naive parquet columns
                    if getattr(column_type, "time_zone", None) is None and getattr(watermark, "tzinfo", None):
                        filter_val = watermark.replace(tzinfo=None)
                    else:
                        filter_val = watermark
                else:
                    # Для остальных типов используем как есть
                    filter_val = watermark
                lf = lf.filter(pl.col(time_column) >= filter_val)

        return lf

    except Exception as e:
        logger.error(
            f"Failed to scan Parquet dataset. "
            f"Paths: {dataset_paths[:3]}... (total {len(dataset_paths)}). "
            f"Error: {str(e)}"
        )
        raise e
