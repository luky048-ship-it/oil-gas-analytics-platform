# plugins/bronze_to_silver/quarantine_writer.py
import logging

import polars as pl
import pyarrow as pa
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
    storage_options: dict = None,
) -> int:
    """
    Записывает невалидные записи в карантин.
    Использует централизованный S3-клиент и обеспечивает строгий контроль ошибок.
    """
    logger.info(
        f"Starting quarantine process for dataset: {dataset}. Reason: {reason_code}"
    )

    try:
        enriched_lf = invalid_lf.with_columns(
            [
                pl.lit(execution_date).alias("_quarantine_execution_date"),
                pl.lit(dataset).alias("_quarantine_source_dataset"),
                pl.lit(execution_date).str.to_date("%Y-%m-%d").alias("partition_date"),
            ]
        )

        arrow_table = enriched_lf.collect().to_arrow()
        q_rows = arrow_table.num_rows

    except Exception as e:
        logger.error(f"Failed to enrich or collect quarantine data for {dataset}: {e}")
        raise AirflowException(f"Quarantine enrichment failed: {e}")

    if q_rows == 0:
        logger.info(f"No invalid records found for dataset {dataset}. Skipping write.")
        return 0

    try:
        s3_fs = get_s3_filesystem()
        pa_fs = PyFileSystem(FSSpecHandler(s3_fs))
    except Exception as e:
        logger.error(f"Failed to initialize S3 filesystem for quarantine: {e}")
        raise AirflowException("Could not connect to S3 for quarantine write.")

    target_dir = f"{base_path.replace('s3://', '')}/{dataset}"
    logger.info(f"Writing {q_rows} records to quarantine at {target_dir}")

    try:
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
    except Exception as e:
        logger.error(
            f"Failed to write parquet to quarantine storage for {dataset}: {e}"
        )
        raise AirflowException(f"IO Error writing to quarantine: {e}")

    logger.info(f"Successfully quarantined {q_rows} rows for {dataset}.")
    return q_rows
