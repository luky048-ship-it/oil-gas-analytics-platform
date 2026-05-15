# plugins/bronze_to_silver/partition_discovery.py
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging

import pyarrow.dataset as ds
from pyarrow import fs

logger = logging.getLogger(__name__)

def discover_incremental_partitions(
    dataset: str,
    watermark: Optional[datetime],
    storage_options: Dict[str, Any],
    bronze_base: str = "s3://datalake/raw",
) -> List[str]:
    """
    Scans the S3 bucket using PyArrow dataset discovery to find all partitions
    where partition_date is greater than or equal to the watermark's date.
    Returns a list of S3 URIs pointing to the relevant partition directories.
    """
    s3_fs = fs.S3FileSystem(
        access_key=storage_options.get("aws_access_key_id"),
        secret_key=storage_options.get("aws_secret_access_key"),
        endpoint_override=storage_options.get("aws_endpoint_url"),
        region=storage_options.get("aws_region", "us-east-1"),
    )

    dataset_path = f"{bronze_base.replace('s3://', '')}/{dataset}"

    try:
        dataset_info = ds.dataset(
            dataset_path, filesystem=s3_fs, format="parquet", partitioning="hive"
        )
    except Exception as e:
        logger.warning(f"Dataset path {dataset_path} not found or inaccessible: {e}")
        return []

    watermark_date = watermark.date() if watermark else None
    valid_partition_paths = set()

    for file_path in dataset_info.files:
        # Extract directory path: e.g. datalake/raw/production/partition_date=2024-01-01
        dir_parts = file_path.split("/")
        if len(dir_parts) <= 1:
            continue

        dir_path = "/".join(dir_parts[:-1])

        if "partition_date=" not in dir_path:
            # Master data tables like 'wells' might not have partition_date
            valid_partition_paths.add(f"s3://{dataset_path}")
            continue

        part_str = dir_path.split("partition_date=")[-1].split("/")[0]

        try:
            part_date = datetime.strptime(part_str, "%Y-%m-%d").date()
            if not watermark_date or part_date >= watermark_date:
                valid_partition_paths.add(f"s3://{dir_path}")
        except ValueError:
            continue

    return sorted(list(valid_partition_paths))
