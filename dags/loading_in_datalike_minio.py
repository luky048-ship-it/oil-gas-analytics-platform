from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

# ---------------------------------------------------------------------------
# Конфигурация таблиц и схем
# ---------------------------------------------------------------------------

TABLE_COLUMNS: Dict[str, List[str]] = {
    "wells": [
        "well_id",
        "name",
        "field_name",
        "region",
        "start_date",
        "operator",
        "status",
    ],
    "production": [
        "prod_id",
        "well_id",
        "date",
        "oil_ton",
        "gas_m3",
        "water_m3",
        "energy_kwh",
        "downtime_hours",
        "temperature",
        "pressure",
    ],
    "well_telemetry": [
        "record_id",
        "well_id",
        "timestamp",
        "pump_speed_rpm",
        "pump_current",
        "pressure_in",
        "pressure_out",
        "temperature",
        "vibration",
        "oil_flow_rate",
    ],
    "well_targets": ["well_id", "date", "daily_oil_ton"],
    "pumps": ["pump_id", "well_id", "type", "install_date", "manufacturer", "model"],
    "pump_sensors": [
        "record_id",
        "pump_id",
        "timestamp",
        "temperature",
        "vibration",
        "current",
        "rpm",
        "pressure",
    ],
    "pump_failures": [
        "failure_id",
        "pump_id",
        "failure_date",
        "failure_type",
        "downtime_hours",
    ],
    "deliveries": [
        "delivery_id",
        "date",
        "source",
        "destination",
        "product_type",
        "volume_ton",
        "cost_usd",
        "delay_hours",
        "distance_km",
        "weather_conditions",
        "driver_id",
        "vehicle_id",
    ],
    "drivers": ["driver_id", "name", "experience_years", "region"],
    "vehicles": ["vehicle_id", "plate_number", "capacity_ton", "fuel_type"],
    "oil_stations": [
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "oil_flow_per_day",
    ],
}

TABLES_CONFIG: Dict[str, Dict[str, Any]] = {
    "well_telemetry": {"date_col": "timestamp", "is_fact": True},
    "production": {"date_col": "date", "is_fact": True},
    "well_targets": {"date_col": "date", "is_fact": True},
    "pump_sensors": {"date_col": "timestamp", "is_fact": True},
    "pump_failures": {"date_col": "failure_date", "is_fact": True},
    "deliveries": {"date_col": "date", "is_fact": True},
    "wells": {"date_col": None, "is_fact": False},
    "pumps": {"date_col": None, "is_fact": False},
    "drivers": {"date_col": None, "is_fact": False},
    "vehicles": {"date_col": None, "is_fact": False},
    "oil_stations": {"date_col": None, "is_fact": False},
}

EXPECTED_SCHEMAS: Dict[str, pa.Schema] = {
    "wells": pa.schema(
        [
            ("well_id", pa.int32()),
            ("name", pa.string()),
            ("field_name", pa.string()),
            ("region", pa.string()),
            ("start_date", pa.date32()),
            ("operator", pa.string()),
            ("status", pa.string()),
        ]
    ),
    "production": pa.schema(
        [
            ("prod_id", pa.int32()),
            ("well_id", pa.int32()),
            ("date", pa.date32()),
            ("oil_ton", pa.decimal128(10, 2)),
            ("gas_m3", pa.decimal128(12, 2)),
            ("water_m3", pa.decimal128(12, 2)),
            ("energy_kwh", pa.decimal128(12, 2)),
            ("downtime_hours", pa.decimal128(5, 2)),
            ("temperature", pa.decimal128(5, 2)),
            ("pressure", pa.decimal128(5, 2)),
        ]
    ),
    "well_telemetry": pa.schema(
        [
            ("record_id", pa.int32()),
            ("well_id", pa.int32()),
            ("timestamp", pa.timestamp("us")),
            ("pump_speed_rpm", pa.decimal128(8, 2)),
            ("pump_current", pa.decimal128(8, 2)),
            ("pressure_in", pa.decimal128(8, 2)),
            ("pressure_out", pa.decimal128(8, 2)),
            ("temperature", pa.decimal128(5, 2)),
            ("vibration", pa.decimal128(5, 2)),
            ("oil_flow_rate", pa.decimal128(8, 2)),
        ]
    ),
    "well_targets": pa.schema(
        [
            ("well_id", pa.int32()),
            ("date", pa.date32()),
            ("daily_oil_ton", pa.decimal128(10, 2)),
        ]
    ),
    "pumps": pa.schema(
        [
            ("pump_id", pa.int32()),
            ("well_id", pa.int32()),
            ("type", pa.string()),
            ("install_date", pa.date32()),
            ("manufacturer", pa.string()),
            ("model", pa.string()),
        ]
    ),
    "pump_sensors": pa.schema(
        [
            ("record_id", pa.int32()),
            ("pump_id", pa.int32()),
            ("timestamp", pa.timestamp("us")),
            ("temperature", pa.decimal128(5, 2)),
            ("vibration", pa.decimal128(5, 2)),
            ("current", pa.decimal128(8, 2)),
            ("rpm", pa.decimal128(8, 2)),
            ("pressure", pa.decimal128(8, 2)),
        ]
    ),
    "pump_failures": pa.schema(
        [
            ("failure_id", pa.int32()),
            ("pump_id", pa.int32()),
            ("failure_date", pa.timestamp("us")),
            ("failure_type", pa.string()),
            ("downtime_hours", pa.decimal128(5, 2)),
        ]
    ),
    "deliveries": pa.schema(
        [
            ("delivery_id", pa.int32()),
            ("date", pa.date32()),
            ("source", pa.string()),
            ("destination", pa.string()),
            ("product_type", pa.string()),
            ("volume_ton", pa.decimal128(10, 2)),
            ("cost_usd", pa.decimal128(10, 2)),
            ("delay_hours", pa.decimal128(6, 2)),
            ("distance_km", pa.decimal128(8, 2)),
            ("weather_conditions", pa.string()),
            ("driver_id", pa.int32()),
            ("vehicle_id", pa.int32()),
        ]
    ),
    "drivers": pa.schema(
        [
            ("driver_id", pa.int32()),
            ("name", pa.string()),
            ("experience_years", pa.int32()),
            ("region", pa.string()),
        ]
    ),
    "vehicles": pa.schema(
        [
            ("vehicle_id", pa.int32()),
            ("plate_number", pa.string()),
            ("capacity_ton", pa.decimal128(8, 2)),
            ("fuel_type", pa.string()),
        ]
    ),
    "oil_stations": pa.schema(
        [
            ("station_id", pa.int32()),
            ("station_name", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("oil_flow_per_day", pa.float64()),
        ]
    ),
}
# ---------------------------------------------------------------------------
# Распределённая блокировка
# ---------------------------------------------------------------------------


def acquire_partition_lock(
    table_name: str, partition_date: str, dag_run_id: str, stale_minutes: int = 30
) -> bool:
    """Пытается установить распределенную блокировку на раздел данных в базе метаданных."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_metadata.loaded_partitions
                    (table_name, partition_date, status, dag_run_id)
                VALUES (%s, %s, 'processing', %s)
                ON CONFLICT (table_name, partition_date) DO NOTHING
                RETURNING status;
            """,
                (table_name, partition_date, dag_run_id),
            )
            if cur.fetchone():
                return True

            cur.execute(
                """
                SELECT status, loaded_at
                FROM etl_metadata.loaded_partitions
                WHERE table_name = %s AND partition_date = %s
                FOR UPDATE;
            """,
                (table_name, partition_date),
            )
            status, loaded_at = cur.fetchone()

            if status == "loaded":
                return False

            if status == "processing":
                now_utc = datetime.now(timezone.utc)
                age = now_utc - loaded_at
                if age > timedelta(minutes=stale_minutes):
                    logging.warning(
                        "Stale lock detected for %s/%s (age %s). Overwriting.",
                        table_name,
                        partition_date,
                        age,
                    )
                    cur.execute(
                        """
                        UPDATE etl_metadata.loaded_partitions
                        SET dag_run_id = %s, loaded_at = CURRENT_TIMESTAMP
                        WHERE table_name = %s AND partition_date = %s
                    """,
                        (dag_run_id, table_name, partition_date),
                    )
                    conn.commit()
                    return True
                else:
                    raise RuntimeError(
                        f"Partition {table_name}/{partition_date} is locked by another run."
                    )
            else:
                raise RuntimeError(f"Unknown partition status: {status}")


def release_partition_lock(table_name: str, partition_date: str, success: bool) -> None:
    """Освобождает ранее установленную блокировку на раздел данных и фиксирует статус (успех/ошибка)."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            if success:
                cur.execute(
                    """
                    UPDATE etl_metadata.loaded_partitions
                    SET status = 'loaded', updated_at = CURRENT_TIMESTAMP
                    WHERE table_name = %s AND partition_date = %s
                """,
                    (table_name, partition_date),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM etl_metadata.loaded_partitions
                    WHERE table_name = %s AND partition_date = %s
                """,
                    (table_name, partition_date),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# S3 / MinIO & Data Quality
# ---------------------------------------------------------------------------


def _get_minio_fs() -> Tuple[s3fs.S3FileSystem, str]:
    """Возвращает настроенный s3fs без конфликта версий aiobotocore."""
    conn = BaseHook.get_connection("aws_default")
    endpoint_url = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

    if not conn.login or not conn.password:
        raise ValueError("MinIO credentials are missing in 'aws_default' connection.")

    fs = s3fs.S3FileSystem(
        key=conn.login,
        secret=conn.password,
        client_kwargs={"endpoint_url": endpoint_url},
        default_block_size=64 * 1024 * 1024,
        default_fill_cache=False,
        use_listings_cache=False,
    )
    return fs, endpoint_url


def _cast_batch_to_schema(
    batch: pa.RecordBatch, target_schema: pa.Schema
) -> pa.RecordBatch:
    """Выполняет приведение типов данных в RecordBatch к ожидаемой схеме Arrow."""
