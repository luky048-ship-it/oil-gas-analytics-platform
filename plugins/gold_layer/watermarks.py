import logging
from datetime import date
from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from gold_layer.constants import POSTGRES_CONN_ID, TABLE_METADATA_WATERMARKS


def get_last_watermark(mart_name: str) -> Optional[date]:
    """Retrieves the latest processed partition date for a given mart."""
    query = f"SELECT MAX(partition_date) FROM {TABLE_METADATA_WATERMARKS} WHERE mart_name = %s"
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    res = hook.get_first(query, parameters=(mart_name,))
    return res[0] if res and res[0] else None


def update_mart_watermark(mart_name: str, partition_date: str, dag_run_id: str):
    """Updates the watermark after a successful load."""
    query = f"""
        INSERT INTO {TABLE_METADATA_WATERMARKS} (mart_name, partition_date, dag_run_id, loaded_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (mart_name, partition_date)
        DO UPDATE SET dag_run_id = EXCLUDED.dag_run_id, loaded_at = CURRENT_TIMESTAMP
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(query, parameters=(mart_name, partition_date, dag_run_id))
    logging.info(f"Watermark updated for {mart_name} on {partition_date}")
