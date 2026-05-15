# plugins/bronze_to_silver/silver_writer.py
from typing import Any, Dict

import polars as pl
import pyarrow.dataset as ds
from pyarrow import fs

from bronze_to_silver.s3_utils import get_s3_storage_options


def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: str,
    silver_base: str = "s3://datalake/silver",
) -> str:
    storage_options = get_s3_storage_options()
    lf_partitioned = lf.with_columns(pl.lit(partition_date).alias("partition_date"))

    s3_fs = fs.S3FileSystem(
        access_key=storage_options.get("aws_access_key_id"),
        secret_key=storage_options.get("aws_secret_access_key"),
        endpoint_override=storage_options.get("aws_endpoint_url"),
        region=storage_options.get("aws_region", "us-east-1"),
    )

    df = lf_partitioned.collect(streaming=True)
    if df.height == 0:
        return f"{silver_base}/{dataset}/partition_date={partition_date}"

    arrow_table = df.to_arrow()
    target_dir = f"{silver_base.replace('s3://', '')}/{dataset}"

    ds.write_dataset(
        data=arrow_table,
        base_dir=target_dir,
        filesystem=s3_fs,
        format="parquet",
        partitioning=ds.partitioning(field_names=["partition_date"]),
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=1024,
    )
    return f"s3://{target_dir}/partition_date={partition_date}"
