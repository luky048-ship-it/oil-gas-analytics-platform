import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from core.s3_connection import get_polars_storage_options

logger = logging.getLogger(__name__)


def get_s3_storage_options(conn_id: str = "aws_default") -> Dict[str, Any]:
    """
    Возвращает словарь с параметрами конфигурации для доступа к S3 хранилищу,
    совместимый с библиотекой Polars.
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
    Загружает данные из Bronze слоя (S3) в ленивом (lazy) режиме.
    Поддерживает фильтрацию по временной отметке (watermark).
    """
    if not dataset_paths:
        logger.warning("load_bronze_dataset called with empty dataset_paths list.")
        return pl.LazyFrame()

    logger.info(
        f"Attempting to scan {len(dataset_paths)} path(s). "
        f"Watermark: {watermark}, Time Column: {time_column}"
    )

    try:
        # Инициализация сканирования Parquet-файлов с поддержкой Hive-партиционирования
        lf = pl.scan_parquet(
            dataset_paths,
            storage_options=storage_options,
            hive_partitioning=True,
        )

        # Применение фильтра инкрементальной загрузки, если задан watermark
        if watermark and time_column:
            logger.info(f"Applying watermark filter: {time_column} >= {watermark}")
            lf = lf.filter(pl.col(time_column) >= watermark)

        return lf

    except Exception as e:
        logger.error(
            f"Failed to scan Parquet dataset. "
            f"Paths: {dataset_paths[:3]}... (total {len(dataset_paths)}). "
            f"Error: {str(e)}"
        )
        raise e
