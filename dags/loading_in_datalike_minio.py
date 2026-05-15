from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

# ---------------------------------------------------------------------------
# Конфигурация таблиц и схем — СИНХРОНИЗИРОВАНО С SQL (init-sql)
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

# Используем double для NUMERIC и int32 для INT
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
            ("oil_ton", pa.float64()),
            ("gas_m3", pa.float64()),
            ("water_m3", pa.float64()),
            ("energy_kwh", pa.float64()),
            ("downtime_hours", pa.float64()),
            ("temperature", pa.float64()),
            ("pressure", pa.float64()),
        ]
    ),
    "well_telemetry": pa.schema(
        [
            ("record_id", pa.int32()),
            ("well_id", pa.int32()),
            ("timestamp", pa.timestamp("s")),
            ("pump_speed_rpm", pa.float64()),
            ("pump_current", pa.float64()),
            ("pressure_in", pa.float64()),
            ("pressure_out", pa.float64()),
            ("temperature", pa.float64()),
            ("vibration", pa.float64()),
            ("oil_flow_rate", pa.float64()),
        ]
    ),
    "well_targets": pa.schema(
        [
            ("well_id", pa.int32()),
            ("date", pa.date32()),
            ("daily_oil_ton", pa.float64()),
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
            ("timestamp", pa.timestamp("s")),
            ("temperature", pa.float64()),
            ("vibration", pa.float64()),
            ("current", pa.float64()),
            ("rpm", pa.float64()),
            ("pressure", pa.float64()),
        ]
    ),
    "pump_failures": pa.schema(
        [
            ("failure_id", pa.int32()),
            ("pump_id", pa.int32()),
            ("failure_date", pa.timestamp("s")),
            ("failure_type", pa.string()),
            ("downtime_hours", pa.float64()),
        ]
    ),
    "deliveries": pa.schema(
        [
            ("delivery_id", pa.int32()),
            ("date", pa.date32()),
            ("source", pa.string()),
            ("destination", pa.string()),
            ("product_type", pa.string()),
            ("volume_ton", pa.float64()),
            ("cost_usd", pa.float64()),
            ("delay_hours", pa.float64()),
            ("distance_km", pa.float64()),
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
            ("capacity_ton", pa.float64()),
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
# Utils
# ---------------------------------------------------------------------------

def _get_minio_fs() -> Tuple[s3fs.S3FileSystem, str]:
    conn = BaseHook.get_connection("aws_default")
    extra = conn.extra_dejson
    return s3fs.S3FileSystem(
        key=conn.login,
        secret=conn.password,
        client_kwargs={"endpoint_url": extra.get("endpoint_url", "http://minio:9000")}
    ), os.getenv("MINIO_DEFAULT_BUCKET", "datalake")

def acquire_partition_lock(
    table_name: str, partition_date: str, dag_run_id: str, stale_minutes: int = 30
) -> bool:
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
            res = cur.fetchone()
            if res:
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
            row = cur.fetchone()
            if not row: return True
            status, loaded_at = row

            if status == "loaded":
                return False

            if status == "processing":
                now_utc = datetime.now(timezone.utc)
                if loaded_at.tzinfo is None:
                    loaded_at = loaded_at.replace(tzinfo=timezone.utc)
                age = now_utc - loaded_at
                if age > timedelta(minutes=stale_minutes):
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
                    raise RuntimeError(f"Partition {table_name}/{partition_date} is locked by {dag_run_id}.")
            return False

def release_partition_lock(table_name: str, partition_date: str, success: bool = True) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    status = "loaded" if success else "failed"
    hook.run(
        """
        UPDATE etl_metadata.loaded_partitions
        SET status = %s, updated_at = CURRENT_TIMESTAMP
        WHERE table_name = %s AND partition_date = %s
    """,
        parameters=(status, table_name, partition_date),
    )

def _cast_batch_to_schema(
    batch: pa.RecordBatch, target_schema: pa.Schema
) -> pa.RecordBatch:
    arrays = []
    for field in target_schema:
        if field.name in batch.schema.names:
            idx = batch.schema.get_field_index(field.name)
            col = batch.column(idx)
            if col.type != field.type:
                try:
                    col = col.cast(field.type, safe=True)
                except pa.ArrowInvalid as e:
                    raise ValueError(f"Schema incompatibility for column '{field.name}': {e}")
        else:
            col = pa.nulls(batch.num_rows, type=field.type)
        arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)

def _invalidate_partition(fs: s3fs.S3FileSystem, partition_dir: str) -> None:
    for marker in ["_SUCCESS", "manifest.json"]:
        path = f"{partition_dir}/{marker}"
        if fs.exists(path):
            fs.rm(path)

def _cleanup_orphans(fs: s3fs.S3FileSystem, partition_dir: str, keep_file: str) -> None:
    if not fs.exists(partition_dir):
        return
    for file_path in fs.ls(partition_dir):
        file_name = file_path.split("/")[-1]
        if file_name.endswith(".parquet") and file_name != keep_file:
            fs.rm(file_path)

def _emit_lineage_event(
    table_name: str, path: str, rows_count: int, run_id: str, schema: pa.Schema
) -> None:
    pass

# ---------------------------------------------------------------------------
# Core ETL Logic
# ---------------------------------------------------------------------------
def extract_load(
    table_name: str, cfg: Dict[str, Any], ds: str, next_ds: str, **context: Any
) -> None:
    dag_run_id = context["dag_run"].run_id
    start_ts = time.time()
    locked = False

    if cfg.get("is_fact"):
        locked = acquire_partition_lock(table_name, ds, dag_run_id)
        if not locked:
            logging.info(f"Partition {table_name}/{ds} already loaded. Skipping.")
            return

    fs, bucket = _get_minio_fs()
    expected_schema = EXPECTED_SCHEMAS[table_name]

    if cfg.get("is_fact"):
        partition_dir = f"{bucket}/raw/{table_name}/partition_date={ds}"
    else:
        partition_dir = f"{bucket}/raw/{table_name}"

    if not fs.exists(partition_dir):
        fs.makedirs(partition_dir)

    file_uuid = uuid.uuid4().hex
    parquet_filename = f"data_{file_uuid}.parquet"
    target_parquet_path = f"{partition_dir}/{parquet_filename}"
    manifest_path = f"{partition_dir}/manifest.json"
    success_path = f"{partition_dir}/_SUCCESS"

    _invalidate_partition(fs, partition_dir)

    columns = TABLE_COLUMNS[table_name]
    col_identifiers = [sql.Identifier(col) for col in columns]
    date_col = cfg.get("date_col")

    if table_name == "well_targets":
        order_clause = sql.SQL("ORDER BY well_id, date")
    else:
        pk_col = columns[0]
        if cfg.get("is_fact") and date_col:
            order_clause = sql.SQL("ORDER BY {}, {}").format(
                sql.Identifier(date_col), sql.Identifier(pk_col)
            )
        else:
            order_clause = sql.SQL("ORDER BY {}").format(sql.Identifier(pk_col))

    if cfg.get("is_fact") and date_col:
        query = sql.SQL("SELECT {} FROM {} WHERE {} >= %s AND {} < %s {}").format(
            sql.SQL(", ").join(col_identifiers),
            sql.Identifier(table_name),
            sql.Identifier(date_col),
            sql.Identifier(date_col),
            order_clause,
        )
        params = (f"{ds} 00:00:00", f"{next_ds} 00:00:00")
    else:
        query = sql.SQL("SELECT {} FROM {} {}").format(
            sql.SQL(", ").join(col_identifiers),
            sql.Identifier(table_name),
            order_clause,
        )
        params = ()

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    rows_count = 0
    writer = None
    s3_file = None

    try:
        with pg_hook.get_conn() as conn:
            cursor_name = f"srv_cur_{table_name}_{uuid.uuid4().hex[:8]}"
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = 100_000
                cur.execute(query, params)

                while True:
                    chunk = cur.fetchmany(100_000)
                    if not chunk:
                        break

                    # Convert result list of tuples to Arrow RecordBatch
                    arrays = [pa.array(col) for col in zip(*chunk)]
                    batch = pa.RecordBatch.from_arrays(arrays, names=columns)
                    batch = _cast_batch_to_schema(batch, expected_schema)
                    table = pa.Table.from_batches([batch])

                    if writer is None:
                        s3_file = fs.open(target_parquet_path, "wb")
                        writer = pq.ParquetWriter(s3_file, schema=expected_schema, compression="snappy")

                    writer.write_table(table)
                    rows_count += len(chunk)

        if rows_count == 0:
            logging.info("Zero rows extracted. Creating empty Parquet.")
            s3_file = fs.open(target_parquet_path, "wb")
            writer = pq.ParquetWriter(s3_file, schema=expected_schema, compression="snappy")
            writer.write_table(pa.Table.from_batches([], schema=expected_schema))

        if writer: writer.close()
        if s3_file: s3_file.close()

        _cleanup_orphans(fs, partition_dir, parquet_filename)

        with fs.open(success_path, "wb") as sf:
            sf.write(b"")

        if locked:
            release_partition_lock(table_name, ds, success=True)
            locked = False

        elapsed = time.time() - start_ts
        logging.info("Committed %s/%s: %d rows in %.2f sec.", table_name, ds, rows_count, elapsed)

    except Exception as e:
        logging.error("ETL failed for %s/%s: %s", table_name, ds, str(e))
        if locked:
            release_partition_lock(table_name, ds, success=False)
        raise

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data_platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="postgres_to_minio_enterprise_2",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=True,
    tags=["enterprise", "etl", "minio", "data-quality"],
    max_active_runs=1,
) as dag:

    for tbl, cfg in TABLES_CONFIG.items():
        PythonOperator(
            task_id=f"extract_load_{tbl}",
            python_callable=extract_load,
            op_kwargs={
                "table_name": tbl,
                "cfg": cfg,
                "ds": "{{ ds }}",
                "next_ds": "{{ next_ds }}",
            },
        )
