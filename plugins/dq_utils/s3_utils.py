# plugins/dq_utils/s3_utils.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import List

import pyarrow.parquet as pq
from airflow.exceptions import AirflowFailException
from core.s3_connection import get_s3_filesystem
from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def discover_available_partitions(
    dataset: str,
    execution_date: str,
    base_path: str = "s3://datalake/raw",
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
        search_pattern = f"{dataset_path}/*{execution_date}*/*.parquet"
        files = fs.glob(search_pattern)

        # Резервный поиск, если структура папок плоская
        if not files:
            search_pattern = f"{dataset_path}/*{execution_date}*.parquet"
            files = fs.glob(search_pattern)
    except Exception as e:
        logger.error(f"Ошибка при поиске файлов в S3: {str(e)}")
        raise AirflowFailException(f"CRITICAL: S3 Listing failed: {str(e)}")

    return [
        f"s3://{str(p)}" if not str(p).startswith("s3://") else str(p)
        for p in files
        if isinstance(p, (str, bytes))
    ]


def validate_file_integrity(dataset: str, partition_path: str) -> DQResult:
    """
    Валидация физического состояния Parquet-файла через PyArrow.
    Не требует конфигурации, берет её напрямую из core.
    """
    fs = get_s3_filesystem()

    try:
        # Проверка существования файла
        if not fs.exists(partition_path):
            raise AirflowFailException(
                f"CRITICAL: File does not exist at {partition_path}"
            )

        # Чтение метаданных для проверки целостности структуры
        with fs.open(partition_path, "rb") as f:
            pf = pq.ParquetFile(f)
            num_rows = pf.metadata.num_rows

            if num_rows == 0:
                logger.warning(f"Файл пуст: {partition_path}")
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


def get_fs(s3_options):
    if s3_options.get("fs"):
        return s3_options["fs"]
    return get_s3_filesystem()
