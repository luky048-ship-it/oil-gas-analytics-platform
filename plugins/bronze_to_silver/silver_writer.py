# plugins/bronze_to_silver/silver_writer.py
import logging

import polars as pl
import pyarrow.dataset as ds
from core.s3_connection import get_s3_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

logger = logging.getLogger(__name__)


def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: str,
    silver_base: str = "s3://datalake/silver",
) -> str:
    """
    Записывает LazyFrame в Silver слой с Hive-партиционированием.
    Использует централизованный s3_filesystem для авторизации.
    """

    lf_partitioned = lf.with_columns(pl.lit(partition_date).alias("partition_date"))

    try:
        s3_fs = get_s3_filesystem()
        pa_fs = PyFileSystem(FSSpecHandler(s3_fs))
    except Exception as e:
        logger.error(f"Failed to initialize S3 filesystem: {e}")
        raise

    df = lf_partitioned.collect()

    if df.height == 0:
        logger.warning(
            f"Dataset {dataset} for {partition_date} is empty. Skipping write."
        )
        return f"{silver_base}/{dataset}/partition_date={partition_date}"

    arrow_table = df.to_arrow()

    clean_base = silver_base.replace("s3://", "")
    target_dir = f"{clean_base}/{dataset}"

    logger.info(
        f"Writing {df.height} rows to {target_dir} for partition {partition_date}"
    )

    ds.write_dataset(
        data=arrow_table,
        base_dir=target_dir,
        filesystem=pa_fs,
        format="parquet",
        partitioning=ds.partitioning(field_names=["partition_date"]),
        existing_data_behavior="overwrite_or_ignore",  # Перезаписываем только целевой раздел
        max_partitions=1024,
    )

    return f"{silver_base}/{dataset}/partition_date={partition_date}"
