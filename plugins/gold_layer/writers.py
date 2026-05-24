# gold_layer/writers.py
import logging
import time
import uuid
from typing import List

import polars as pl
from adbc_driver_postgresql import dbapi as adbc_dbapi
from gold_layer.config import MartSpec
from gold_layer.connections import get_postgres_uri, get_psycopg2_conn
from gold_layer.constants import STAGING_SCHEMA
from gold_layer.models import MartBuildResult
from gold_layer.sql_templates import (DELETE_PARTITION_FROM_GOLD,
                                      DROP_STAGING_TABLE,
                                      INSERT_FROM_STAGING_TO_GOLD)

logger = logging.getLogger(__name__)


def validate_dataframe(df: pl.DataFrame, spec: MartSpec) -> None:
    """Обеспечение качества данных перед фиксацией транзакции."""
    if df.is_empty():
        return

    logger.info("Executing Data Quality checks for '%s'...", spec.table_name)

    for pk_col in spec.primary_key:
        null_count = df.select(pl.col(pk_col).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(f"DQ Error: PK '{pk_col}' contains {null_count} NULLs.")

    if spec.primary_key:
        duplicates = (
            df.group_by(spec.primary_key).len().filter(pl.col("len") > 1).height
        )
        if duplicates > 0:
            raise ValueError(
                f"DQ Error: Found {duplicates} duplicated PK combinations."
            )

    if spec.business_rules:
        ctx = pl.SQLContext()
        ctx.register("df", df.lazy())

        for rule in spec.business_rules:
            try:
                query = f"SELECT count(*) FROM df WHERE NOT ({rule.rule})"
                res = ctx.execute(query)
                failed_rows = (
                    res.collect().item()
                    if isinstance(res, pl.LazyFrame)
                    else res.item()
                )

                if failed_rows > 0:
                    msg = f"DQ Error: {failed_rows} rows failed rule '{rule.rule}'"
                    if rule.severity in ("CRITICAL", "HIGH"):
                        raise ValueError(msg)
                    logger.warning(msg)
            except Exception as e:
                logger.error("Failed to evaluate rule '%s': %s", rule.rule, e)
                if isinstance(e, ValueError) or (
                    hasattr(rule, "severity") and rule.severity in ("CRITICAL", "HIGH")
                ):
                    raise ValueError(
                        f"DQ Evaluation aborted due to error in high-severity rule '{rule.rule}': {e}"
                    ) from e


def write_mart(
    df: pl.DataFrame, spec: MartSpec, partition_dates: List[str]
) -> MartBuildResult:
    start_time = time.time()
    processed_rows = len(df)
    partition_dates_str = [str(d) for d in partition_dates]

    if processed_rows == 0:
        return MartBuildResult(
            mart_name=spec.table_name,
            processed_rows=0,
            inserted_rows=0,
            execution_time_sec=time.time() - start_time,
            partition_date=(
                ",".join(partition_dates_str) if partition_dates_str else "None"
            ),
            watermark=None,  # type: ignore
        )

    target_table = spec.table_name
    raw_table_name = target_table.split(".")[-1]

    run_hash = str(uuid.uuid4())[:8]
    staging_table_name = f"stg_{raw_table_name}_{run_hash}"
    staging_table_full = f"{STAGING_SCHEMA}.{staging_table_name}"

    inserted_rows = 0
    conn = get_psycopg2_conn()

    try:
        validate_dataframe(df, spec)

        excluded_cols = {"mart_id", "record_id", "load_timestamp", "partition_date"}
        business_columns = [col for col in df.columns if col not in excluded_cols]
        columns_sql_str = ", ".join(business_columns)

        df_to_load = df.select(business_columns)

        logger.info(
            "Loading %d rows to '%s' via ADBC (mode='create')...",
            processed_rows,
            staging_table_full,
        )
        with adbc_dbapi.connect(get_postgres_uri()) as adbc_conn:
            with adbc_conn.cursor() as adbc_cur:
                adbc_cur.adbc_ingest(
                    table_name=staging_table_name,
                    data=df_to_load.to_arrow(),
                    mode="create",
                    db_schema_name=STAGING_SCHEMA,
                )
            adbc_conn.commit()

        with conn:
            with conn.cursor() as cur:
                logger.info(
                    "Deleting partitions from gold table: %s", partition_dates_str
                )
                cur.execute(
                    DELETE_PARTITION_FROM_GOLD.format(target_table=target_table),
                    (partition_dates_str,),
                )

                insert_query = INSERT_FROM_STAGING_TO_GOLD.format(
                    target_table=target_table,
                    staging_table=staging_table_full,
                    columns=columns_sql_str,
                )
                logger.info(
                    "Inserting records from staging into gold table with implicit cast..."
                )
                cur.execute(insert_query)
                inserted_rows = cur.rowcount

    finally:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        DROP_STAGING_TABLE.format(staging_table=staging_table_full)
                    )
        except Exception as e:
            logger.warning(
                "Failed to drop staging table '%s': %s", staging_table_full, e
            )
        finally:
            conn.close()

    exec_time = time.time() - start_time
    logger.info(
        "Successfully updated '%s'. Inserted: %d (%.2fs)",
        target_table,
        inserted_rows,
        exec_time,
    )

    return MartBuildResult(
        mart_name=spec.table_name,
        processed_rows=processed_rows,
        inserted_rows=inserted_rows,
        execution_time_sec=exec_time,
        partition_date=",".join(partition_dates_str),
        watermark=None,  # type: ignore
    )
