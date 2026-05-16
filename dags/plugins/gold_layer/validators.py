import polars as pl
import logging
from typing import List

def validate_business_readiness(lf: pl.LazyFrame, mart_name: str) -> pl.LazyFrame:
    """
    Performs business-level validation on the LazyFrame.
    Logs warnings for data quality issues but doesn't necessarily fail.
    """
    # Example: Check for negative production
    if "oil_ton" in lf.columns:
        neg_count = lf.filter(pl.col("oil_ton") < 0).collect().height
        if neg_count > 0:
            logging.warning(f"[{mart_name}] Found {neg_count} rows with negative oil_ton")

    # Add more business checks as needed
    return lf

def validate_mart_before_publish(df: pl.DataFrame, mart_name: str):
    """
    Strict validation on the materialized DataFrame before it goes to staging.
    Fails the task if critical rules are violated.
    """
    # 1. Null check on critical columns
    critical_cols = {
        "mart_production": ["well_id", "date"],
        "mart_well_kpi": ["well_id", "date"],
        "mart_failures": ["pump_id", "date", "timestamp"],
        "mart_logistics": ["delivery_id", "date"]
    }

    cols = critical_cols.get(mart_name, [])
    for col in cols:
        if col in df.columns:
            null_count = df.filter(pl.col(col).is_null()).height
            if null_count > 0:
                raise ValueError(f"CRITICAL: {mart_name} has {null_count} NULLs in critical column {col}")

    # 2. Row count check
    if df.height == 0:
        logging.warning(f"[{mart_name}] No rows to publish for this batch.")

    logging.info(f"[{mart_name}] Validation passed for {df.height} rows.")
