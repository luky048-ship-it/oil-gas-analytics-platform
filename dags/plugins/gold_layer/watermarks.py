import logging
from datetime import datetime, date
from typing import Optional, List
from dags.plugins.gold_layer.connections import get_psycopg2_conn
from dags.plugins.gold_layer.constants import TABLE_METADATA_WATERMARKS

def get_last_watermark(mart_name: str) -> Optional[date]:
    """Retrieves the latest processed partition date for a given mart."""
    query = f"SELECT MAX(partition_date) FROM {TABLE_METADATA_WATERMARKS} WHERE mart_name = %s"
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (mart_name,))
            res = cur.fetchone()
            return res[0] if res and res[0] else None

def update_mart_watermark(mart_name: str, partition_date: str, dag_run_id: str):
    """Updates the watermark after a successful load. Must be called within the same transaction or after commit."""
    query = f"""
        INSERT INTO {TABLE_METADATA_WATERMARKS} (mart_name, partition_date, dag_run_id, loaded_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (mart_name, partition_date)
        DO UPDATE SET dag_run_id = EXCLUDED.dag_run_id, loaded_at = CURRENT_TIMESTAMP
    """
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (mart_name, partition_date, dag_run_id))
        conn.commit()
    logging.info(f"Watermark updated for {mart_name} on {partition_date}")
