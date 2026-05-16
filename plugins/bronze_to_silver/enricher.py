# plugins/bronze_to_silver/enricher.py
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
    Lazily joins the primary dataset with a reference dataset (e.g., wells, pumps).
    Assumes the reference dataset is stored in Silver and already deduplicated.
    """
    # Load the reference dataset lazily
    ref_lf = pl.scan_parquet(
        f"{reference_dataset}/**/*.parquet",
        storage_options=storage_options,
        hive_partitioning=True,
    )

    # Perform lazy join
    enriched_lf = lf.join(ref_lf, on=join_key, how=how)

    return enriched_lf
