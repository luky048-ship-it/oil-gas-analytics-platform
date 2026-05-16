# plugins/bronze_to_silver/quarantine_writer.py
from typing import Any, Dict

import polars as pl
import pyarrow.dataset as ds
from pyarrow import fs


def write_quarantine_dataset(
    invalid_lf: pl.LazyFrame,
    dataset: str,
    reason_code: str,
    execution_date: str,
    storage_options: Dict[str, Any],
    base_path: str = "s3://datalake/quarantine",
) -> int:
    """
    Enriches invalid records with metadata and writes them to the Quarantine layer.
    Returns the number of quarantined rows.
    """
    # Enrich with execution metadata
    enriched_lf = invalid_lf.with_columns(
        [
            pl.lit(execution_date).alias("_quarantine_execution_date"),
            pl.lit(dataset).alias("_quarantine_source_dataset"),
            pl.lit(execution_date).alias("partition_date"),  # For Hive partitioning
        ]
    )

    # Materialize the invalid records (expected to be a small subset)
    invalid_df = enriched_lf.collect()
    q_rows = invalid_df.height

    if q_rows == 0:
        return 0

    # Configure PyArrow S3 filesystem
    s3_fs = fs.S3FileSystem(
        access_key=storage_options.get("aws_access_key_id"),
        secret_key=storage_options.get("aws_secret_access_key"),
        endpoint_override=storage_options.get("aws_endpoint_url"),
        region=storage_options.get("aws_region", "us-east-1"),
    )

    # Convert to Arrow Table and write
    arrow_table = invalid_df.to_arrow()
    target_dir = f"{base_path.replace('s3://', '')}/{dataset}"

    ds.write_dataset(
        data=arrow_table,
        base_dir=target_dir,
        filesystem=s3_fs,
        format="parquet",
        partitioning=ds.partitioning(field_names=["partition_date"]),
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=1024,
    )

    return q_rows
