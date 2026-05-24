# /plugins/bronze_to_silver/metadata_utils.py
import logging
from datetime import datetime
from typing import List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from bronze_to_silver.pipeline_execution import PipelineExecutionResult
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# ЕДИНЫЙ коннекшн к базе данных, где лежит схема etl_metadata
DEFAULT_CONN_ID = "postgres_default"


def get_postgres_connection(conn_id: str = DEFAULT_CONN_ID):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()


# ---------------------------------------------------------------------------
# 1. PARTITION DISCOVERY (Чтение метаданных Bronze)
# ---------------------------------------------------------------------------


def get_bronze_partitions_from_db(
    table_name: str,
    start_date: str,
    end_date: str,
    is_fact: bool,
    conn_id: str = DEFAULT_CONN_ID,
) -> List[str]:
    """
    Читает таблицу etl_metadata.loaded_partitions и возвращает список
    S3 путей (file_path) для успешных загрузок Bronze.
    """
    if not is_fact:
        return [f"s3://datalake/bronze/{table_name}/"]

    query = """
        SELECT file_path 
        FROM etl_metadata.loaded_partitions
        WHERE table_name = %s 
          AND partition_date BETWEEN %s AND %s
          AND status = 'loaded'
          AND file_path IS NOT NULL
        ORDER BY partition_date;
    """

    hook = PostgresHook(postgres_conn_id=conn_id)
    records = hook.get_records(query, parameters=(table_name, start_date, end_date))

    paths = []
    for row in records:
        path = row[0]
        if path and not path.startswith("s3://"):
            path = f"s3://{path}"
        if path:
            paths.append(path)

    logger.info(
        f"Found {len(paths)} loaded partitions for {table_name} ({start_date} to {end_date})"
    )
    return paths


# ---------------------------------------------------------------------------
# 2. WATERMARK MANAGEMENT (Для обработки late-arriving data)
# ---------------------------------------------------------------------------


def get_last_watermark(
    dataset: str, conn_id: str = DEFAULT_CONN_ID
) -> Optional[datetime]:
    query = "SELECT last_processed_watermark FROM etl_metadata.pipeline_watermarks WHERE dataset = %s;"
    with get_postgres_connection(conn_id) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dataset,))
            result = cursor.fetchone()
    return result[0] if result else None


def update_pipeline_watermark(
    dataset: str,
    watermark: datetime,
    conn_id: str = DEFAULT_CONN_ID,
) -> None:
    """
    Обновляет вотермарк для отслеживания инкрементальной обработки.
    Параметр `execution_date` удален как избыточный.
    """
    query = """
        INSERT INTO etl_metadata.pipeline_watermarks (dataset, last_processed_watermark, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (dataset) DO UPDATE SET
            last_processed_watermark = GREATEST(etl_metadata.pipeline_watermarks.last_processed_watermark, EXCLUDED.last_processed_watermark),
            updated_at = NOW();
    """
    with get_postgres_connection(conn_id) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dataset, watermark))
        conn.commit()


# ---------------------------------------------------------------------------
# 3. AUDIT & OBSERVABILITY (Логирование метрик пайплайна)
# ---------------------------------------------------------------------------


def publish_pipeline_metadata(
    result: PipelineExecutionResult, conn_id: str = DEFAULT_CONN_ID
) -> None:
    query = """
        INSERT INTO etl_metadata.pipeline_executions (
            dataset, partition_date, processed_rows, quarantined_rows,
            execution_time_sec, watermark, status
        ) VALUES %s
        ON CONFLICT (dataset, partition_date) DO UPDATE SET
            processed_rows = EXCLUDED.processed_rows,
            quarantined_rows = EXCLUDED.quarantined_rows,
            execution_time_sec = EXCLUDED.execution_time_sec,
            watermark = EXCLUDED.watermark,
            status = EXCLUDED.status,
            updated_at = NOW();
    """
    values = [
        (
            result.dataset,
            result.partition_date,
            result.processed_rows,
            result.quarantined_rows,
            result.execution_time_sec,
            result.watermark,
            "SUCCESS",
        )
    ]
    with get_postgres_connection(conn_id) as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, query, values)
        conn.commit()
