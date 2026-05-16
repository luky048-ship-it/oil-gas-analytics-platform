import polars as pl
import pyarrow as pa
import logging
from adbc_driver_postgresql import dbapi as adbc_dbapi
from gold_layer.connections import get_postgres_uri, get_psycopg2_conn
from gold_layer.constants import STAGING_SCHEMA
from gold_layer.sql_templates import (
    CREATE_STAGING_TABLE, DROP_STAGING_TABLE,
    DELETE_PARTITION_FROM_GOLD, INSERT_FROM_STAGING_TO_GOLD
)

def write_staging_mart(df: pl.DataFrame, mart_name: str) -> str:
    """
    Writes the DataFrame to a staging table in Postgres using ADBC.
    Returns the name of the staging table.
    """
    staging_table = f"{STAGING_SCHEMA}.stg_{mart_name}"
    target_table = f"gold.{mart_name}"
    uri = get_postgres_uri()

    # 1. Create/Truncate staging table
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_STAGING_TABLE.format(
                staging_table=staging_table,
                target_table=target_table
            ))
        conn.commit()

    # 2. Write data using ADBC for high performance
    # Convert Polars DataFrame to Arrow Table
    arrow_table = df.to_arrow()

    with adbc_dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            # ADBC specific ingestion
            # We use 'append' since we already created/truncated the table
            cur.adbc_ingest(staging_table, arrow_table, mode="append")

    logging.info(f"Loaded {len(df)} rows into {staging_table}")
    return staging_table

def atomic_partition_overwrite(mart_name: str, staging_table: str, partition_dates: list):
    """
    Transactional DELETE + INSERT for each partition_date.
    Ensures that Gold layer is updated atomically for all affected dates.
    """
    target_table = f"gold.{mart_name}"

    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            for dt in partition_dates:
                logging.info(f"Overwriting partition {dt} in {target_table}")
                # Atomic swap for this date
                cur.execute(DELETE_PARTITION_FROM_GOLD.format(target_table=target_table), (dt,))
                cur.execute(INSERT_FROM_STAGING_TO_GOLD.format(
                    target_table=target_table,
                    staging_table=staging_table
                ), (dt,))
        conn.commit()
    logging.info(f"Successfully updated {target_table} for dates: {partition_dates}")

def cleanup_staging(staging_table: str):
    """Drops the staging table."""
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DROP_STAGING_TABLE.format(staging_table=staging_table))
        conn.commit()
