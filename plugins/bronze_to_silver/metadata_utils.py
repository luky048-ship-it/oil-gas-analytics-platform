import logging
from datetime import datetime
from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from bronze_to_silver.pipeline_execution import PipelineExecutionResult
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_postgres_connection(conn_id: str = "postgres_default"):
    """Инициализирует и возвращает соединение с базой данных Postgres через Airflow Hook."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()


def get_last_watermark(
    dataset: str, conn_id: str = "postgres_default"
) -> Optional[datetime]:
    """
    Извлекает значение последней успешно обработанной временной отметки (watermark)
    для указанного набора данных из таблицы метаданных.
    """
    query = """
        SELECT last_processed_watermark
        FROM etl_metadata.pipeline_watermarks
        WHERE dataset = %s;
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        result = hook.get_first(query, parameters=(dataset,))
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Failed to fetch watermark for {dataset}: {e}")
        return None


def update_pipeline_watermark(
    dataset: str,
    watermark: datetime,
    execution_date: str,
    conn_id: str = "postgres_default",
) -> None:
    """
    Выполняет атомарное обновление (UPSERT) временной отметки прогресса (watermark)
    для набора данных. Новое значение применяется только если оно больше текущего.
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


def publish_pipeline_metadata(
    result: PipelineExecutionResult, conn_id: str = "postgres_default"
) -> None:
    """
    Публикует метрики выполнения этапа Bronze-to-Silver (количество строк, время обработки, статус)
    в системную таблицу метаданных.
    """
    query = """
        INSERT INTO etl_metadata.pipeline_executions (
            dataset, partition_date, processed_rows, quarantined_rows,
            execution_time_sec, watermark, status, updated_at
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
