import logging
from datetime import date
from typing import Any, List, Optional

import polars as pl
from core.s3_connection import get_polars_storage_options, get_s3_filesystem

from gold_layer.connections import get_postgres_uri, get_psycopg2_conn
from gold_layer.constants import S3_BUCKET, SILVER_PREFIX

logger = logging.getLogger(__name__)


def discover_new_partitions(
    table_name: str, last_watermark: Optional[date]
) -> List[str]:
    """
    Определяет список новых разделов (partitions) для обработки, основываясь на данных
    о прохождении проверок качества (DQ) в метаданных Postgres.
    """
    query = """
        SELECT partition_date 
        FROM etl_metadata.dq_pipeline_runs 
        WHERE dataset = %s 
          AND status = 'SUCCESS'
    """
    params: List[Any] = [table_name]

    # Добавление фильтрации по дате, если задан watermark
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
                new_dates.append(row[0].strftime("%Y-%m-%d"))

    if new_dates:
        logger.info(
            f"[{table_name}] Discovered {len(new_dates)} new partitions: {new_dates}"
        )
    else:
        logger.info(f"[{table_name}] No new DQ-validated partitions found.")

    return new_dates


def load_silver_dataset(
    table_name: str,
    partition_dates: Optional[List[str]] = None,
    pk_columns: Optional[List[str]] = None,
) -> pl.LazyFrame:
    """
    Загружает данные из Silver слоя для указанных дат.
    Выполняет операцию anti-join с данными в карантине для исключения ошибочных записей
    на этапе формирования витрин.
    """
    polars_opts = get_polars_storage_options()
    fs = get_s3_filesystem()

    base_path = f"{SILVER_PREFIX}/{table_name}"
    quarantine_base = f"s3://{S3_BUCKET}/quarantine/{table_name}"

    # Загрузка всех данных, если даты не указаны
    if not partition_dates:
        path = f"{base_path}/**/*.parquet"
        return pl.scan_parquet(path, storage_options=polars_opts)

    silver_paths = []
    # Формирование путей к существующим файлам в S3
    for dt in partition_dates:
        path_mask = f"{base_path}/partition_date={dt}/*.parquet"

        if fs.glob(path_mask.replace("s3://", "")):
            silver_paths.append(path_mask)

    if not silver_paths:
        logger.warning(
            f"[{table_name}] No Silver files found for dates: {partition_dates}"
        )
        return pl.LazyFrame()

    lf_silver = pl.scan_parquet(silver_paths, storage_options=polars_opts)

    if not pk_columns:
        return lf_silver

    # Исключение записей, попавших в карантин
    quarantine_paths = []
    for dt in partition_dates:
        q_path = f"{quarantine_base}/partition_date={dt}/*.parquet"
        if fs.glob(q_path.replace("s3://", "")):
            quarantine_paths.append(q_path)

    if quarantine_paths:
        lf_quarantine = pl.scan_parquet(quarantine_paths, storage_options=polars_opts)

        lf_clean = lf_silver.join(
            lf_quarantine.select(pk_columns), on=pk_columns, how="anti"
        )
        logger.info(f"[{table_name}] Applied Quarantine Anti-Join on keys {pk_columns}")
        return lf_clean

    return lf_silver


def load_gold_dataset(table_name: str) -> pl.LazyFrame:
    """
    Загружает исторические данные из целевой таблицы Gold-слоя (Postgres)
    для использования в инкрементальных расчетах или расчете KPI.
    """
    uri = get_postgres_uri()
    try:
        df = pl.read_database(query=f"SELECT * FROM {table_name}", connection=uri)
        return df.lazy()
    except Exception as e:
        logger.error(f"Failed to load gold dataset {table_name}: {str(e)}")
        raise
