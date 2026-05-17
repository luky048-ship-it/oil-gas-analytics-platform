import logging
from datetime import date, datetime
from typing import List, Optional

import polars as pl
from gold_layer.connections import get_psycopg2_conn, get_s3_fs
from gold_layer.constants import S3_BUCKET, SILVER_PREFIX


def discover_new_partitions(
    table_name: str, last_watermark: Optional[date]
) -> List[str]:
    query = """
        SELECT partition_date 
        FROM etl_metadata.dq_pipeline_runs 
        WHERE dataset = %s 
          AND status = 'SUCCESS'
    """
    params = [table_name]

    if last_watermark:
        query += " AND partition_date > %s"
        params.append(last_watermark)

    query += " ORDER BY partition_date ASC;"

    new_dates = []
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            results = cur.fetchall()
            for row in results:
                # row[0] is a datetime.date object
                new_dates.append(row[0].strftime("%Y-%m-%d"))

    if new_dates:
        logging.info(
            f"[{table_name}] Discovered {len(new_dates)} DQ-validated partitions: {new_dates}"
        )
    else:
        logging.info(f"[{table_name}] No new DQ-validated partitions found.")

    return new_dates


def load_silver_dataset(
    table_name: str,
    partition_dates: Optional[List[str]] = None,
    pk_columns: Optional[List[str]] = None,
) -> pl.LazyFrame:
    fs = get_s3_fs()
    base_path = f"{SILVER_PREFIX}/{table_name}"
    quarantine_base = f"s3://{S3_BUCKET}/quarantine/{table_name}"

    if not partition_dates:
        path = f"{base_path}/**/*.parquet"
        return pl.scan_parquet(path, storage_options=fs.storage_options)

    # 1. Формируем пути для Silver
    silver_paths = []
    for dt in partition_dates:
        p = f"{base_path}/partition_date={dt}/*.parquet"
        if fs.glob(p.replace("s3://", "")):
            silver_paths.append(p)

    if not silver_paths:
        logging.warning(f"No Silver files found for {table_name} in {partition_dates}")
        return pl.LazyFrame()

    lf_silver = pl.scan_parquet(silver_paths, storage_options=fs.storage_options)

    # 2. Если не переданы ключи для Anti-Join, возвращаем Silver как есть
    if not pk_columns:
        return lf_silver

    # 3. Ищем Карантинные файлы для этих же дат
    quarantine_paths = []
    for dt in partition_dates:
        q_p = f"{quarantine_base}/partition_date={dt}/*.parquet"
        if fs.glob(q_p.replace("s3://", "")):
            quarantine_paths.append(q_p)

    # 4. Если есть карантин, делаем Anti-Join
    if quarantine_paths:
        lf_quarantine = pl.scan_parquet(
            quarantine_paths, storage_options=fs.storage_options
        )

        # Вычитаем карантин из Silver
        lf_clean = lf_silver.join(
            lf_quarantine.select(pk_columns), on=pk_columns, how="anti"
        )
        logging.info(
            f"[{table_name}] Applied Quarantine Anti-Join on keys {pk_columns}"
        )
        return lf_clean

    return lf_silver


def load_gold_dataset(table_name: str) -> pl.LazyFrame:
    """
    Loads a Gold dataset from Postgres as a Polars LazyFrame.
    """
    from gold_layer.connections import get_postgres_uri

    uri = get_postgres_uri()
    df = pl.read_database(query=f"SELECT * FROM {table_name}", connection=uri)
    return df.lazy()
