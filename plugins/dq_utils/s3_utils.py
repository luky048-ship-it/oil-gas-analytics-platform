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
    Выполняет поиск доступных разделов (partitions) Parquet для заданного набора данных и даты.
    Автоматически переключается между Hive-структурой и плоской структурой папок.
    """
    # Инициализация клиента S3
    fs = get_s3_filesystem()

    bucket_path = base_path.replace("s3://", "").rstrip("/")
    dataset_path = f"{bucket_path}/{dataset}"

    try:
        # Попытка поиска файлов с использованием стандартного шаблона даты
        search_pattern = f"{dataset_path}/*{execution_date}*/*.parquet"
        files = fs.glob(search_pattern)

        # Резервный поиск для упрощенных структур хранения
        if not files:
            search_pattern = f"{dataset_path}/*{execution_date}*.parquet"
            files = fs.glob(search_pattern)
    except Exception as e:
        logger.error(f"S3 listing error: {str(e)}")
        raise AirflowFailException(f"CRITICAL: S3 Listing failed: {str(e)}")

    return [
        f"s3://{str(p)}" if not str(p).startswith("s3://") else str(p)
        for p in files
        if isinstance(p, (str, bytes))
    ]


def validate_file_integrity(dataset: str, partition_path: str) -> DQResult:
    """
    Проверяет физическую целостность и читаемость Parquet-файла.
    Использует метаданные PyArrow для подтверждения структуры файла без полной загрузки данных.
    """
    fs = get_s3_filesystem()

    try:
        # Проверка фактического существования объекта в хранилище
        if not fs.exists(partition_path):
            raise AirflowFailException(
                f"CRITICAL: File does not exist at {partition_path}"
            )

        # Анализ структуры Parquet-файла
        with fs.open(partition_path, "rb") as f:
            pf = pq.ParquetFile(f)
            num_rows = pf.metadata.num_rows

            if num_rows == 0:
                logger.warning(f"File is empty: {partition_path}")
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
    """
    Вспомогательная функция для получения инстанса файловой системы S3
    из переданных опций или создания нового.
    """
    if s3_options.get("fs"):
        return s3_options["fs"]
    return get_s3_filesystem()
