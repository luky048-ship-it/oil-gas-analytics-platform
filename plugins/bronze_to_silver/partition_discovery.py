# plugins/bronze_to_silver/partition_discovery.py
import logging
import os
from datetime import datetime
from typing import List, Optional

from core.s3_connection import get_s3_filesystem

logger = logging.getLogger(__name__)


def discover_incremental_partitions(
    dataset: str,
    watermark: Optional[datetime],
    storage_options: dict = None,  # Оставлено для совместимости, но не используется
    bronze_base: str = "s3://datalake/raw",
) -> List[str]:
    """
    Обнаруживает новые партиции в S3, основываясь на водном знаке (watermark).

    Senior Note:
    - Использует централизованный S3 клиент.
    - Реализует защищенный парсинг путей.
    - Детально логгирует процесс обнаружения.
    """

    # 1. Инициализация клиента
    try:
        fs_client = get_s3_filesystem()
    except Exception as e:
        logger.critical(
            f"Failed to initialize S3 filesystem for discovery: {e}", exc_info=True
        )
        return []

    # 2. Формирование пути
    # Убираем s3:// для s3fs glob
    bucket_relative_path = bronze_base.replace("s3://", "")
    dataset_path = os.path.join(bucket_relative_path, dataset)

    logger.info(f"Scanning S3 path: {dataset_path} for dataset: {dataset}")

    # 3. Получение списка файлов
    try:
        raw_files = fs_client.glob(f"{dataset_path}/**/*.parquet")
    except (PermissionError, FileNotFoundError) as e:
        logger.error(f"Access error scanning {dataset_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error during glob operation on {dataset_path}: {e}")
        return []

    if not raw_files:
        logger.warning(f"No files found in {dataset_path}. Check bucket path.")
        return []

    logger.debug(f"Found {len(raw_files)} files in dataset {dataset}")

    # 4. Логика фильтрации
    watermark_date = watermark.date() if watermark else None
    valid_partition_paths = set()

    for file_path in raw_files:
        # Получаем директорию файла
        dir_path = os.path.dirname(file_path)

        # Если нет партиций (файл лежит сразу в dataset_path), считаем его валидным
        if "partition_date=" not in dir_path:
            valid_partition_paths.add(f"s3://{dataset_path}")
            continue

        # Безопасный парсинг даты
        try:
            # Ищем подстроку партиции
            part_part = [
                p for p in dir_path.split("/") if p.startswith("partition_date=")
            ]
            if not part_part:
                continue

            part_str = part_part[0].replace("partition_date=", "")
            part_date = datetime.strptime(part_str, "%Y-%m-%d").date()

            # Фильтрация по watermark
            if not watermark_date or part_date >= watermark_date:
                valid_partition_paths.add(f"s3://{dir_path}")

        except (ValueError, IndexError) as e:
            logger.warning(f"Could not parse partition date from path {dir_path}: {e}")
            continue

    discovered = sorted(list(valid_partition_paths))
    logger.info(
        f"Discovery complete. Found {len(discovered)} valid partitions for {dataset}."
    )

    return discovered
