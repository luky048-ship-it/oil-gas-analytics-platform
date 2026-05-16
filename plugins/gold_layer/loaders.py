import polars as pl
import logging
from datetime import datetime, date
from typing import List, Optional
from gold_layer.constants import SILVER_PREFIX
from gold_layer.connections import get_s3_fs

def load_silver_dataset(table_name: str, partition_dates: Optional[List[str]] = None) -> pl.LazyFrame:
    """
    Loads a Silver dataset as a Polars LazyFrame.
    If partition_dates is provided, it filters by partition_date.
    Uses scan_parquet for efficiency.
    """
    fs = get_s3_fs()
    base_path = f"{SILVER_PREFIX}/{table_name}"

    # We use glob pattern to scan all partitions if no specific dates are provided
    # Structure: silver/{table_name}/partition_date=YYYY-MM-DD/*.parquet
    if partition_dates:
        paths = [f"{base_path}/partition_date={dt}/*.parquet" for dt in partition_dates]
        # Check which paths actually exist to avoid Polars error
        existing_paths = []
        for p in paths:
            if fs.glob(p.replace("s3://", "")):
                existing_paths.append(p)

        if not existing_paths:
            logging.warning(f"No parquet files found for {table_name} in partitions {partition_dates}")
            return pl.LazyFrame()

        return pl.scan_parquet(existing_paths, storage_options=fs.storage_options)
    else:
        path = f"{base_path}/**/*.parquet"
        return pl.scan_parquet(path, storage_options=fs.storage_options)

def discover_new_partitions(table_name: str, last_watermark: Optional[date]) -> List[str]:
    """
    Lists directories in S3 and returns partition dates that are newer than last_watermark.
    """
    fs = get_s3_fs()
    base_path = f"{SILVER_PREFIX.replace('s3://', '')}/{table_name}"

    try:
        partition_dirs = fs.ls(base_path)
    except FileNotFoundError:
        logging.error(f"Silver table {table_name} not found at {base_path}")
        return []

    new_dates = []
    for d in partition_dirs:
        # Expected format: silver/table/partition_date=YYYY-MM-DD
        if "partition_date=" in d:
            date_str = d.split("partition_date=")[-1]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                if last_watermark is None or dt > last_watermark:
                    new_dates.append(date_str)
            except ValueError:
                continue

    return sorted(new_dates)

def load_gold_dataset(table_name: str) -> pl.LazyFrame:
    """
    Loads a Gold dataset from Postgres as a Polars LazyFrame.
    """
    from gold_layer.connections import get_postgres_uri
    uri = get_postgres_uri()
    df = pl.read_database(f"SELECT * FROM {table_name}", connection=uri)
    return df.lazy()
