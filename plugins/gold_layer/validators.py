import polars as pl
import logging
from gold_layer.config import MART_CONTRACTS

def validate_business_readiness(lf: pl.LazyFrame, mart_name: str) -> pl.LazyFrame:
    """
    Performs business-level validation on the LazyFrame.
    Logs warnings for data quality issues but doesn't necessarily fail.
    """
    contract = MART_CONTRACTS.get(mart_name)
    if not contract:
        return lf

    rules = contract.business_rules

    # Example: Check for negative production
    if "min_oil_ton" in rules and "oil_ton" in lf.collect_schema().names():
        neg_count = lf.filter(pl.col("oil_ton") < rules["min_oil_ton"]).collect().height
        if neg_count > 0:
            logging.warning(f"[{mart_name}] Found {neg_count} rows with oil_ton < {rules['min_oil_ton']}")

    return lf

def validate_mart_before_publish(df: pl.DataFrame, mart_name: str):
    """
    Strict validation on the materialized DataFrame before it goes to staging.
    Fails the task if critical rules are violated.
    """
    contract = MART_CONTRACTS.get(mart_name)
    if not contract:
        logging.warning(f"No contract found for mart {mart_name}")
        return

    # 1. Null check on critical columns
    for col in contract.critical_columns:
        if col in df.columns:
            null_count = df.filter(pl.col(col).is_null()).height
            if null_count > 0:
                raise ValueError(f"CRITICAL: {mart_name} has {null_count} NULLs in critical column {col}")

    # 2. Row count check
    if df.height == 0:
        logging.warning(f"[{mart_name}] No rows to publish for this batch.")

    logging.info(f"[{mart_name}] Validation passed for {df.height} rows.")
