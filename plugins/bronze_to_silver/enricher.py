from typing import Any, Dict

import polars as pl


def enrich_reference_data(
    lf: pl.LazyFrame,
    reference_dataset: str,
    join_key: str,
    storage_options: Dict[str, Any],
    how: str = "left",
) -> pl.LazyFrame:
    """
    Выполняет ленивое (lazy) объединение основного набора данных со справочником (например, скважины, насосы).
    Предполагается, что справочник хранится в Silver слое и уже прошел дедупликацию.
    """
    # Загрузка справочного набора данных в режиме LazyFrame
    ref_lf = pl.scan_parquet(
        f"{reference_dataset}/**/*.parquet",
        storage_options=storage_options,
        hive_partitioning=True,
    )

    # Выполнение объединения данных
    enriched_lf = lf.join(ref_lf, on=join_key, how=how)

    return enriched_lf
