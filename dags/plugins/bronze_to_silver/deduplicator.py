# plugins/bronze_to_silver/deduplicator.py
from typing import List, Optional

import polars as pl


def deduplicate_dataset(
    lf: pl.LazyFrame, key_columns: List[str], timestamp_column: Optional[str]
) -> pl.LazyFrame:
    """
    Lazily deduplicates records based on natural keys.
    If a timestamp column is provided, keeps the latest record.
    """
    if not key_columns:
        return lf

    if timestamp_column:
        # Sort by timestamp to ensure the 'last' record is the most recent
        lf = lf.sort(timestamp_column)

    # Unique keeps the last row for each subset combination
    return lf.unique(subset=key_columns, keep="last")
