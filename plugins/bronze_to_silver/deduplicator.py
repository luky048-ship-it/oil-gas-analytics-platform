# plugins/bronze_to_silver/deduplicator.py
from typing import List, Optional

import polars as pl


def deduplicate_dataset(
    lf: pl.LazyFrame, key_columns: List[str], timestamp_column: Optional[str]
) -> pl.LazyFrame:
    """
    Лениво дедуплицирует записи на основе естественных ключей.
    Если указана колонка с временной меткой, сохраняет самую последнюю (актуальную) запись.
    """
    if not key_columns:
        return lf

    if timestamp_column:
        lf = lf.sort(timestamp_column)

    return lf.unique(subset=key_columns, keep="last", maintain_order=True)
