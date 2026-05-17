# plugins/bronze_to_silver/partition_discovery.py
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast  # << Добавили cast

import s3fs

logger = logging.getLogger(__name__)


def discover_incremental_partitions(
    dataset: str,
    watermark: Optional[datetime],
    storage_options: Dict[str, Any],
    bronze_base: str = "s3://datalake/raw",
) -> List[str]:

    try:
        fs_client = s3fs.S3FileSystem(
            key=storage_options.get("aws_access_key_id"),
            secret=storage_options.get("aws_secret_access_key"),
            client_kwargs={"endpoint_url": storage_options.get("aws_endpoint_url")},
        )

        dataset_path = f"{bronze_base.replace('s3://', '')}/{dataset}"

        raw_files = fs_client.glob(f"{dataset_path}/**/*.parquet")
        all_files = cast(List[str], raw_files)

    except Exception as e:
        logger.warning(f"Error accessing S3: {e}")
        return []

    watermark_date = watermark.date() if watermark else None
    valid_partition_paths = set()

    for file_path in all_files:
        path_parts = file_path.split("/")
        if len(path_parts) < 2:
            continue

        dir_path = "/".join(path_parts[:-1])

        if "partition_date=" not in dir_path:
            valid_partition_paths.add(f"s3://{dataset_path}")
            continue

        try:
            part_str = dir_path.split("partition_date=")[-1].split("/")[0]
            part_date = datetime.strptime(part_str, "%Y-%m-%d").date()
            if not watermark_date or part_date >= watermark_date:
                valid_partition_paths.add(f"s3://{dir_path}")
        except (ValueError, IndexError):
            continue

    return sorted(list(valid_partition_paths))
