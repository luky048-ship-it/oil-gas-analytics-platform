from __future__ import annotations

import logging
from typing import Optional

import polars as pl
from dq_utils.s3_utils import get_s3fs_client

logger = logging.getLogger(__name__)


def write_quarantine_dataset(
    invalid_df: pl.DataFrame,
    dataset: str,
    validation_name: str,
    partition_date: str,
    s3_options: dict,
    base_path: str = "s3://datalake/quarantine",
) -> Optional[str]:
    """
    Сохраняет записи, не прошедшие проверку качества, в отдельную зону карантина в S3.
    Обогащает данные техническими столбцами с указанием причины и контекста ошибки.
    """
    if invalid_df.height == 0:
        logger.info(
            f"No invalid records to quarantine for dataset '{dataset}' on {partition_date}."
        )
        return None

    # Добавление метаданных для анализа причин попадания в карантин
    quarantine_df = invalid_df.with_columns(
        [
            pl.lit("validation_failed").alias("__reason_code"),
            pl.lit(dataset).alias("__source_dataset"),
            pl.lit(validation_name).alias("__validation_name"),
            pl.lit(partition_date).alias("__execution_date"),
        ]
    )

    # Формирование целевого пути с учетом партиционирования
    target_dir = f"{base_path.replace('s3://', '').rstrip('/')}/{dataset}/partition_date={partition_date}"
    target_path = (
        f"{target_dir}/quarantined_{validation_name.replace(' ', '_').lower()}.parquet"
    )

    try:
        # Прямая запись в S3 через pyarrow
        fs = get_s3fs_client(s3_options)
        quarantine_df.write_parquet(
            target_path,
            compression="snappy",
            use_pyarrow=True,
            pyarrow_options={"filesystem": fs},
        )

        logger.warning(
            f"Quarantined {quarantine_df.height} invalid records for '{dataset}'. "
            f"Validation: '{validation_name}'. Path: {target_path}"
        )

        return target_path

    except Exception as e:
        error_msg = f"CRITICAL: Failed to write quarantine dataset to {target_path}. Error: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e
