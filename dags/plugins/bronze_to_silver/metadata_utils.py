# plugins/bronze_to_silver/metadata_utils.py
from datetime import datetime
from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from bronze_to_silver.pipeline_execution import PipelineExecutionResult


def get_postgres_connection(conn_id: str = "etl_metadata_db"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()


def get_last_watermark(
    dataset: str, conn_id: str = "etl_metadata_db"
) -> Optional[datetime]:
    """
    Retrieves the maximum processed event_time for a given dataset.
    """
    query = """
        SELECT last_processed_watermark
        FROM etl_metadata.pipeline_watermarks
        WHERE dataset = %s;
    """
    with get_postgres_connection(conn_id) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dataset,))
            result = cursor.fetchone()

    return result[0] if result else None


def update_pipeline_watermark(
    dataset: str,
    watermark: datetime,
    execution_date: str,
    conn_id: str = "etl_metadata_db",
) -> None:
    """
    Atomically UPSERTs the new watermark into the metadata database.
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
    result: PipelineExecutionResult, conn_id: str = "etl_metadata_db"
) -> None:
    """
    Publishes the execution metrics of the Bronze-to-Silver pipeline step.
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
