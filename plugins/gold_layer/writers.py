# gold_layer/writers.py
import logging
import time
from typing import List

import polars as pl
import pyarrow as pa
from adbc_driver_postgresql import dbapi as adbc_dbapi

from gold_layer.config import MartSpec
from gold_layer.connections import get_postgres_uri, get_psycopg2_conn
from gold_layer.constants import GOLD_SCHEMA, STAGING_SCHEMA
from gold_layer.models import MartBuildResult
from gold_layer.sql_templates import (
    CREATE_STAGING_TABLE,
    DELETE_PARTITION_FROM_GOLD,
    DROP_STAGING_TABLE,
    INSERT_FROM_STAGING_TO_GOLD,
)

logger = logging.getLogger(__name__)


def validate_dataframe(df: pl.DataFrame, spec: MartSpec) -> None:
    """
    Data Quality First: Проверяет бизнес-правила (BusinessRules) в in-memory фрейме
    до того, как данные начнут грузиться в БД.
    """
    if df.is_empty():
        return

    logger.info("Executing Data Quality checks for '%s'...", spec.table_name)

    for pk_col in spec.primary_key:
        null_count = df.select(pl.col(pk_col).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(
                f"Data Quality Error: Primary key '{pk_col}' contains {null_count} NULLs."
            )

    if spec.primary_key:
        duplicates = (
            df.group_by(spec.primary_key).len().filter(pl.col("len") > 1).height
        )
        if duplicates > 0:
            raise ValueError(
                f"Data Quality Error: Found {duplicates} duplicated PK combinations."
            )

    for rule in spec.business_rules:
        try:
            ctx = pl.SQLContext(frames={"df": df})
            failed_rows = ctx.execute(
                f"SELECT count(*) FROM df WHERE NOT ({rule.rule})"
            ).item()

            if failed_rows > 0:
                msg = (
                    f"Data Quality Error: {failed_rows} rows failed rule '{rule.rule}'"
                )
                if rule.severity == "CRITICAL" or rule.severity == "HIGH":
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            logger.error("Failed to parse rule '%s': %s", rule.rule, e)


def create_staging_table(target_table: str, staging_table: str) -> None:
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                CREATE_STAGING_TABLE.format(
                    staging_table=staging_table, target_table=target_table
                )
            )
        conn.commit()
    logger.debug("Staging table '%s' is ready.", staging_table)


def load_to_staging_adbc(df: pl.DataFrame, staging_table: str) -> None:
    uri = get_postgres_uri()
    arrow_table = df.to_arrow()

    schema_name, table_name = staging_table.split(".")

    with adbc_dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema_name};")

            cur.adbc_ingest(table_name, arrow_table, mode="append")


def execute_atomic_swap(
    target_table: str, staging_table: str, partition_dates: List[str]
) -> int:
    inserted_rows = 0
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            logger.info("Swapping partitions: %s", partition_dates)

            cur.execute(
                DELETE_PARTITION_FROM_GOLD.format(target_table=target_table),
                (partition_dates,),
            )

            cur.execute(
                INSERT_FROM_STAGING_TO_GOLD.format(
                    target_table=target_table, staging_table=staging_table
                ),
                (partition_dates,),
            )
            inserted_rows = cur.rowcount

        conn.commit()
    return inserted_rows


def cleanup_staging(staging_table: str) -> None:
    try:
        with get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DROP_STAGING_TABLE.format(staging_table=staging_table))
            conn.commit()
    except Exception as e:
        logger.warning("Failed to drop staging table '%s': %s", staging_table, e)


def write_mart(
    df: pl.DataFrame, spec: MartSpec, partition_dates: List[str]
) -> MartBuildResult:
    """
    Главная точка входа для сохранения витрины.
    Реализует паттерн Blue-Green на уровне партиций.
    """
    start_time = time.time()
    processed_rows = len(df)

    if processed_rows == 0:
        logger.info(
            "No data to write for '%s'. Skipping DB operations.", spec.table_name
        )
        return MartBuildResult(
            mart_name=spec.table_name,
            processed_rows=0,
            inserted_rows=0,
            execution_time_sec=time.time() - start_time,
            partition_date=",".join(partition_dates) if partition_dates else "None",
            watermark=None,
        )

    target_table = spec.table_name
    raw_table_name = target_table.split(".")[-1]
    staging_table = f"{STAGING_SCHEMA}.stg_{raw_table_name}"

    try:
        validate_dataframe(df, spec)

        create_staging_table(target_table, staging_table)

        logger.info(
            "Loading %d rows to '%s' via ADBC...", processed_rows, staging_table
        )
        load_to_staging_adbc(df, staging_table)

        inserted_rows = execute_atomic_swap(
            target_table, staging_table, partition_dates
        )

    finally:
        cleanup_staging(staging_table)

    exec_time = time.time() - start_time
    logger.info(
        "Successfully updated '%s'. Processed: %d, Inserted: %d (Time: %.2fs)",
        target_table,
        processed_rows,
        inserted_rows,
        exec_time,
    )

    return MartBuildResult(
        mart_name=spec.table_name,
        processed_rows=processed_rows,
        inserted_rows=inserted_rows,
        execution_time_sec=exec_time,
        partition_date=",".join(partition_dates),
        watermark=None,
    )
