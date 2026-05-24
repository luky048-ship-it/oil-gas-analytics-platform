# /plugins/bronze_to_silver/quarantine_writer.py
from typing import Any, Dict

import polars as pl


def write_quarantine_dataset(
    invalid_lf: pl.LazyFrame,
    dataset: str,
    reason_code: str,
    execution_date: str,
    storage_options: Dict[str, Any],
    base_path: str = "s3://datalake/quarantine",
) -> int:
    """
    Обогащает невалидные записи метаданными и записывает их в слой Quarantine.
    """
    enriched_lf = invalid_lf.with_columns(
        [
            pl.lit(execution_date).alias("_quarantine_execution_date"),
            pl.lit(dataset).alias("_quarantine_source_dataset"),
            pl.lit(reason_code).alias("_quarantine_reason_code"),
        ]
    )

    invalid_df = enriched_lf.collect()
    q_rows = invalid_df.height

    if q_rows == 0:
        return 0

    target_path = f"{base_path}/{dataset}/partition_date={execution_date}/data.parquet"

    invalid_df.write_parquet(
        target_path,
        storage_options=storage_options,
    )

    return q_rows
