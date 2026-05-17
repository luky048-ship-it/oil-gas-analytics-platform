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
            ("timestamp", pa.timestamp("s")),
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
            ("timestamp", pa.timestamp("s")),
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
            ("failure_date", pa.timestamp("s")),
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
    arrays = []
    for field in target_schema:
        if field.name in batch.schema.names:
            idx = batch.schema.get_field_index(field.name)
            col = batch.column(idx)
            if col.type != field.type:
                try:
                    # strict validation (safe=True)
                    col = col.cast(field.type, safe=True)
                except pa.ArrowInvalid as e:
                    logging.error(
                        "Schema incompatibility: field '%s' cannot be safely cast to %s",
                        field.name,
                        field.type,
                    )
                    raise ValueError(
                        f"Schema incompatibility for column '{field.name}': {e}"
                    )
        else:
            col = pa.nulls(batch.num_rows, type=field.type)
        arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


def _invalidate_partition(fs: s3fs.S3FileSystem, partition_dir: str) -> None:
    """Удаляет _SUCCESS и manifest.json для блокировки чтения downstream системами."""
    for marker in ["_SUCCESS", "manifest.json"]:
        path = f"{partition_dir}/{marker}"
        if fs.exists(path):
            fs.rm(path)


def _cleanup_orphans(fs: s3fs.S3FileSystem, partition_dir: str, keep_file: str) -> None:
    """Удаляет все parquet файлы в партиции, кроме текущего успешно записанного."""
    if not fs.exists(partition_dir):
        return
    for file_path in fs.ls(partition_dir):
        file_name = file_path.split("/")[-1]
        if file_name.endswith(".parquet") and file_name != keep_file:
            logging.info("Cleaning up orphaned/old file: %s", file_path)
            fs.rm(file_path)


def _emit_lineage_event(
    table_name: str, path: str, rows_count: int, run_id: str, schema: pa.Schema
) -> None:
    """Production-grade lineage hook (OpenLineage-ready)."""
    schema_fields = [{"name": f.name, "type": str(f.type)} for f in schema]
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {"runId": run_id},
        "job": {"namespace": "postgres_to_minio", "name": f"extract_{table_name}"},
        "inputs": [{"namespace": "postgres", "name": table_name}],
        "outputs": [
            {
                "namespace": "s3",
                "name": path,
                "facets": {
                    "schema": {"fields": schema_fields},
                    "stats": {"rowCount": rows_count},
                },
            }
        ],
    }
    logging.info("OpenLineage Event: %s", json.dumps(event))


# ---------------------------------------------------------------------------
# Core ETL Logic
# ---------------------------------------------------------------------------
def extract_load(
    table_name: str, cfg: Dict[str, Any], ds: str, next_ds: str, **context: Any
) -> None:
    dag_run_id = context["dag_run"].run_id
    start_ts = time.time()
    locked = False

    locked = False
    if cfg.get("is_fact"):
        locked = acquire_partition_lock(table_name, ds, dag_run_id)
        if not locked:
            return

    fs, _ = _get_minio_fs()
    bucket = os.getenv("MINIO_DEFAULT_BUCKET", "datalake")
    expected_schema = EXPECTED_SCHEMAS[table_name]

    if cfg.get("is_fact"):
        partition_dir = f"{bucket}/raw/{table_name}/partition_date={ds}"
    else:
        partition_dir = f"{bucket}/raw/{table_name}"

    file_uuid = uuid.uuid4().hex
    parquet_filename = f"data_{file_uuid}.parquet"
    target_parquet_path = f"{partition_dir}/{parquet_filename}"
    manifest_path = f"{partition_dir}/manifest.json"
    success_path = f"{partition_dir}/_SUCCESS"

    _invalidate_partition(fs, partition_dir)

    columns = TABLE_COLUMNS[table_name]
    col_identifiers = [sql.Identifier(col) for col in columns]
    date_col = cfg.get("date_col")

    # Deterministic ordering
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

                    arrays = [pa.array(col) for col in zip(*chunk)]
                    batch = pa.RecordBatch.from_arrays(arrays, names=columns)
                    batch = _cast_batch_to_schema(batch, expected_schema)
                    table = pa.Table.from_batches([batch])

                    if writer is None:
                        s3_file = fs.open(target_parquet_path, "wb")
                        writer = pq.ParquetWriter(
                            s3_file,
                            schema=expected_schema,
                            compression="snappy",
                            use_dictionary=True,
                            data_page_size=1048576,
                            write_batch_size=100000,
                        )

                    writer.write_table(table)
                    rows_count += len(chunk)

        if rows_count == 0:
            logging.info("Zero rows extracted. Creating empty Parquet.")
            s3_file = fs.open(target_parquet_path, "wb")
            writer = pq.ParquetWriter(
                s3_file, schema=expected_schema, compression="snappy"
            )
            writer.write_table(expected_schema.empty_table())

        if writer:
            writer.close()
        if s3_file:
            s3_file.close()

        file_info = fs.info(target_parquet_path)
        file_size = file_info["size"]
        pf = pq.ParquetFile(fs.open(target_parquet_path, "rb"))
        if pf.metadata.num_rows != rows_count:
            raise ValueError(
                f"Integrity check failed: expected {rows_count} rows, got {pf.metadata.num_rows}"
            )

        _cleanup_orphans(fs, partition_dir, parquet_filename)

        schema_hash = hashlib.sha256(expected_schema.to_string().encode()).hexdigest()
        manifest = {
            "file_path": target_parquet_path,
            "row_count": rows_count,
            "schema_hash": schema_hash,
            "creation_timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_run_id": dag_run_id,
            "file_size_bytes": file_size,
        }
        with fs.open(manifest_path, "w") as mf:
            json.dump(manifest, mf)

        with fs.open(success_path, "wb") as sf:
            sf.write(b"")

        if locked:
            release_partition_lock(table_name, ds, success=True)
            locked = False

        _emit_lineage_event(
            table_name, partition_dir, rows_count, dag_run_id, expected_schema
        )

        elapsed = time.time() - start_ts
        logging.info(
            "Committed %s/%s: %d rows in %.2f sec.", table_name, ds, rows_count, elapsed
        )

    except Exception as e:
        logging.error("ETL failed for %s/%s: %s", table_name, ds, str(e))
        if locked:
            try:
                release_partition_lock(table_name, ds, success=False)
                locked = False
            except Exception as unlock_err:
                logging.error("Failed to release lock: %s", unlock_err)
        if fs.exists(target_parquet_path):
            fs.rm(target_parquet_path)
        raise

    finally:
        if locked:
            try:
                release_partition_lock(table_name, ds, success=False)
            except Exception:
                logging.exception("Failed to release lock in finally")
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if s3_file is not None:
            try:
                s3_file.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="postgres_to_minio",
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
