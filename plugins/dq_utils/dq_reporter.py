from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    dataset: str
    validation_type: str
    status: str
    failed_rows: int
    checked_rows: int
    message: str
    created_at: datetime


def persist_dq_results(
    results: List[Dict[str, Any]],
    execution_date: str,
    postgres_conn_id: str = "postgres_default",
) -> None:
    if not results:
        return

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    insert_query = """
        INSERT INTO etl_metadata.dq_validation_results (
            dataset, validation_type, partition_date, execution_date,
            status, failed_rows, checked_rows, message, created_at, updated_at
        ) VALUES %s
        ON CONFLICT (dataset, validation_type, partition_date, execution_date)
        DO UPDATE SET
            status = EXCLUDED.status,
            failed_rows = EXCLUDED.failed_rows,
            checked_rows = EXCLUDED.checked_rows,
            message = EXCLUDED.message,
            updated_at = EXCLUDED.updated_at;
    """
    now = datetime.now(timezone.utc).isoformat()

    records = [
        (
            r["dataset"],
            r["validation_type"],
            execution_date,
            execution_date,
            r["status"],
            r["failed_rows"],
            r["checked_rows"],
            r["message"],
            r["created_at"],
            now,
        )
        for r in results
    ]

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, records)
            conn.commit()
