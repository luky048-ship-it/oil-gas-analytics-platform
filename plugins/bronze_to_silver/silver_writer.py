# plugins/bronze_to_silver/silver_writer.py
import logging

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from core.s3_connection import get_s3_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

logger = logging.getLogger(__name__)


def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: str,
    silver_base: str = "s3://datalake/silver",
    storage_options: dict = None,
    time_column: str = None,
) -> str:
    """
    Записывает LazyFrame в Silver слой с Hive-партиционированием.
    Партиция строится из Event Time данных, а не из даты запуска DAG.
    """

    # Если указан time_column, используем его для партиционирования (Event Time)
    # Это предотвращает проблемы с Late Arriving Data
    if time_column:
        try:
            lf_partitioned = lf.with_columns(
                pl.col(time_column).dt.date().alias("partition_date")
            )
        except Exception:
            # Если не удалось извлечь дату из time_column, fallback на execution_date
            logger.warning(
                f"Failed to extract date from {time_column}, using execution_date as fallback"
            )
            lf_partitioned = lf.with_columns(
                pl.lit(partition_date).str.to_date("%Y-%m-%d").alias("partition_date")
            )
    else:
        lf_partitioned = lf.with_columns(
            pl.lit(partition_date).str.to_date("%Y-%m-%d").alias("partition_date")
        )

    try:
        s3_fs = get_s3_filesystem()
        pa_fs = PyFileSystem(FSSpecHandler(s3_fs))
    except Exception as e:
        logger.error(f"Failed to initialize S3 filesystem: {e}")
        raise

    arrow_table = lf_partitioned.collect().to_arrow()

    if arrow_table.num_rows == 0:
        logger.warning(
            f"Dataset {dataset} for {partition_date} is empty. Skipping write."
        )
        return f"{silver_base}/{dataset}/partition_date={partition_date}"

    clean_base = silver_base.replace("s3://", "")
    target_dir = f"{clean_base}/{dataset}"

    logger.info(
        f"Writing {arrow_table.num_rows} rows to {target_dir} for partition {partition_date}"
    )

    ds.write_dataset(
        data=arrow_table,
        base_dir=target_dir,
        filesystem=pa_fs,
        format="parquet",
        partitioning=ds.partitioning(
            schema=pa.schema([("partition_date", pa.date32())])
        ),
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=1024,
    )

    return f"{silver_base}/{dataset}/partition_date={partition_date}"
