# plugins/dq_utils/s3_utils.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pyarrow.dataset as ds
from airflow.exceptions import AirflowFailException
from core.s3_connection import get_s3_filesystem
from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def discover_available_partitions(
    dataset: str,
    execution_date: str,
    base_path: str = "s3://datalake/silver",
    s3_options: Optional[dict] = None,
) -> List[str]:
    """
    Выполняет поиск партиций Parquet для заданного датасета и даты.
    Использует централизованный объект файловой системы S3.
    """
    # Получаем авторизованный инстанс через core-модуль
    fs = get_s3_filesystem()
    bucket_path = base_path.replace("s3://", "").rstrip("/")
    dataset_path = f"{bucket_path}/{dataset}"

    try:
        # Поиск файлов с учетом партиционирования по дате
        search_pattern = f"{dataset_path}/*{execution_date}*/"
        files = fs.glob(search_pattern)

    except Exception as e:
        logger.error(f"Ошибка при поиске файлов в S3: {str(e)}")
        raise AirflowFailException(f"CRITICAL: S3 Listing failed: {str(e)}")

    return [
        f"s3://{str(p)}" if not str(p).startswith("s3://") else str(p)
        for p in files
        if isinstance(p, (str, bytes))
    ]


def validate_file_integrity(
    dataset: str,
    partition_path: str,
    s3_options: Optional[dict] = None,
) -> DQResult:
    """
    Валидация физического состояния Parquet-файла через PyArrow.
    Не требует конфигурации, берет её напрямую из core.
    """
    fs = get_s3_filesystem()
    path_without_s3 = partition_path.replace("s3://", "").rstrip("/")

    try:
        parquet_ds = ds.dataset(path_without_s3, filesystem=fs, format="parquet")

        files = parquet_ds.files
        if not files:
            return DQResult(
                dataset=dataset,
                validation_type="Physical Integrity",
                status="FAIL",
                failed_rows=0,
                checked_rows=0,
                message=f"Partition folder is empty: {partition_path}",
                created_at=datetime.utcnow(),
            )

        try:
            scan_check = parquet_ds.to_batches(batch_size=1)
            next(scan_check)
        except StopIteration:
            pass

        num_rows = parquet_ds.count_rows()

        return DQResult(
            dataset=dataset,
            validation_type="Physical Integrity",
            status="PASS",
            failed_rows=0,
            checked_rows=num_rows,
            message=f"Validated {len(files)} files. Total rows: {num_rows}",
            created_at=datetime.utcnow(),
        )

    except Exception as e:
        error_msg = (
            f"CRITICAL: Integrity check failed for {partition_path}. Error: {str(e)}"
        )
        logger.error(error_msg)
        raise AirflowFailException(error_msg)


def get_fs(s3_options):
    if s3_options.get("fs"):
        return s3_options["fs"]
    return get_s3_filesystem()
