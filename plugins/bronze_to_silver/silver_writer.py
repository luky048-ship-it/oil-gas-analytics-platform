# plugins/bronze_to_silver/silver_writer.py
from typing import Any, Dict, Optional

import polars as pl
import pyarrow.dataset as ds
import s3fs
from bronze_to_silver.s3_utils import get_s3_storage_options
from pyarrow.fs import FSSpecHandler, PyFileSystem


def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: str,
    storage_options: Optional[Dict[str, Any]] = None,
    silver_base: str = "s3://datalake/silver",
) -> str:
    if storage_options is None:
        storage_options = get_s3_storage_options()

    lf_partitioned = lf.with_columns(pl.lit(partition_date).alias("partition_date"))

    s3 = s3fs.S3FileSystem(
        key=storage_options.get("aws_access_key_id"),
        secret=storage_options.get("aws_secret_access_key"),
        client_kwargs={"endpoint_url": storage_options.get("aws_endpoint_url")},
    )
    pa_fs = PyFileSystem(FSSpecHandler(s3))

    df = lf_partitioned.collect()

    if df.height == 0:
        return f"{silver_base}/{dataset}/partition_date={partition_date}"

    arrow_table = df.to_arrow()
    target_dir = f"{silver_base.replace('s3://', '')}/{dataset}"

    ds.write_dataset(
        data=arrow_table,
        base_dir=target_dir,
        filesystem=pa_fs,
        format="parquet",
        partitioning=ds.partitioning(field_names=["partition_date"]),
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=1024,
    )

    return f"s3://{target_dir}/partition_date={partition_date}"
