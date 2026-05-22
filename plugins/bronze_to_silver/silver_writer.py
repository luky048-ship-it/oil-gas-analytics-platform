# plugins/bronze_to_silver/silver_writer.py
from typing import Any, Dict, Optional

import polars as pl


def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: Optional[str],
    storage_options: Dict[str, Any],
    silver_base: str = "s3://datalake/silver",
) -> str:
    """
    Записывает LazyFrame в Silver слой, используя sink_parquet для избежания OOM.
    Для фактов формирует Hive-партицию по дате, для измерений пишет в один файл.
    """
    base_dir = f"{silver_base}/{dataset}"

    if partition_date:
        target_path = f"{base_dir}/partition_date={partition_date}/data.parquet"
    else:
        target_path = f"{base_dir}/data.parquet"

    lf.sink_parquet(
        target_path,
        storage_options=storage_options,
    )

    return target_path
