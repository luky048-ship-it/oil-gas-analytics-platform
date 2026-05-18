import logging

import polars as pl
import pyarrow.dataset as ds
from airflow.exceptions import AirflowException
from core.s3_connection import get_s3_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

logger = logging.getLogger(__name__)


def write_quarantine_dataset(
    invalid_lf: pl.LazyFrame,
    dataset: str,
    reason_code: str,
    execution_date: str,
    base_path: str = "s3://datalake/quarantine",
) -> int:
    """
    Выполняет запись невалидных записей (аномалий, нарушений схемы) в зону карантина в S3.
    Добавляет метаданные для последующего анализа причин исключения данных из основного потока.
    """
    logger.info(
        f"Starting quarantine process for dataset: {dataset}. Reason: {reason_code}"
    )

    # 1. Обогащение данных техническими столбцами для карантина
    try:
        enriched_lf = invalid_lf.with_columns(
            [
                pl.lit(execution_date).alias("_quarantine_execution_date"),
                pl.lit(dataset).alias("_quarantine_source_dataset"),
                pl.lit(execution_date).alias("partition_date"),
            ]
        )

        # Материализация ошибочных записей
        invalid_df = enriched_lf.collect()
        q_rows = invalid_df.height
    except Exception as e:
        logger.error(f"Failed to enrich or collect quarantine data for {dataset}: {e}")
        raise AirflowException(f"Quarantine enrichment failed: {e}")

    if q_rows == 0:
        logger.info(f"No invalid records found for dataset {dataset}. Skipping write.")
        return 0

    # 2. Инициализация файловой системы S3
    try:
        s3_fs = get_s3_filesystem()
        pa_fs = PyFileSystem(FSSpecHandler(s3_fs))
    except Exception as e:
        logger.error(f"Failed to initialize S3 filesystem for quarantine: {e}")
        raise AirflowException("Could not connect to S3 for quarantine write.")

    # 3. Пакетная запись данных в формате Parquet с Hive-партиционированием
    target_dir = f"{base_path.replace('s3://', '')}/{dataset}"
    logger.info(f"Writing {q_rows} records to quarantine at {target_dir}")

    try:
        ds.write_dataset(
            data=invalid_df.to_arrow(),
            base_dir=target_dir,
            filesystem=pa_fs,
            format="parquet",
            partitioning=ds.partitioning(field_names=["partition_date"]),
            existing_data_behavior="overwrite_or_ignore",
            max_partitions=1024,
        )
    except Exception as e:
        logger.error(
            f"Failed to write parquet to quarantine storage for {dataset}: {e}"
        )
        raise AirflowException(f"IO Error writing to quarantine: {e}")

    logger.info(f"Successfully quarantined {q_rows} rows for {dataset}.")
    return q_rows
