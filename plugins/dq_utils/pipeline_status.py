from __future__ import annotations

import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def publish_pipeline_status(
    dataset: str,
    execution_date: str,
    status: str,
    postgres_conn_id: str = "postgres_default",
) -> None:
    """
    Публикует итоговый статус проверки качества данных в таблицу метаданных (etl_metadata.dq_pipeline_runs).
    Используется для координации зависимых DAG и подтверждения готовности данных.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Применение UPSERT для обновления статуса текущего запуска
    upsert_query = """
        INSERT INTO etl_metadata.dq_pipeline_runs (
            dataset,
            partition_date,
            execution_date,
            status,
            created_at,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset, partition_date)
        DO UPDATE SET
            status = EXCLUDED.status,
            execution_date = EXCLUDED.execution_date,
            updated_at = EXCLUDED.updated_at;
    """

    now = datetime.utcnow()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                upsert_query,
                (dataset, execution_date, execution_date, status, now, now),
            )
            conn.commit()

    logger.info(
        f"Published pipeline status '{status}' for dataset '{dataset}' at {execution_date}."
    )
