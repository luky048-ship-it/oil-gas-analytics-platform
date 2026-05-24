# dags/loading_in_datalike_minio.py
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

logger = logging.getLogger(__name__)

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
            ("timestamp", pa.timestamp("us")),
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
            ("timestamp", pa.timestamp("us")),
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
            ("failure_date", pa.timestamp("us")),
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
# Работа с MinIO / S3
# ---------------------------------------------------------------------------


def get_s3_filesystem(conn_id: str = "aws_default") -> s3fs.S3FileSystem:
    try:
        conn = BaseHook.get_connection(conn_id)
        endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")
        use_ssl = not endpoint.startswith("http://")
        logger.info(f"Initializing s3fs.S3FileSystem for {endpoint} (SSL: {use_ssl})")
        return s3fs.S3FileSystem(
            key=conn.login,
            secret=conn.password,
            client_kwargs={"endpoint_url": endpoint},
            use_ssl=use_ssl,
        )
    except Exception as e:
        logger.error(f"Failed to initialize S3FileSystem: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Распределённая блокировка
# ---------------------------------------------------------------------------


def acquire_partition_lock(
    table_name: str,
    partition_date: str,
    dag_run_id: str,
    stale_minutes: int = 30,
) -> bool:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_metadata.loaded_partitions
                    (table_name, partition_date, status, dag_run_id, loaded_at, updated_at)
                VALUES (%s, %s, 'processing', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (table_name, partition_date) DO NOTHING
                RETURNING status
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
                FOR UPDATE
                """,
                (table_name, partition_date),
            )
            row = cur.fetchone()
            if not row:
                return False
            status, loaded_at = row

            if status == "loaded":
                return False

            if status == "processing":
                now_utc = datetime.now(timezone.utc)
                if loaded_at.tzinfo is None:
                    loaded_at = loaded_at.replace(tzinfo=timezone.utc)
                age = now_utc - loaded_at
                if age > timedelta(minutes=stale_minutes):
                    logger.warning(
                        "Stale lock for %s/%s (age=%s), overwriting",
                        table_name,
                        partition_date,
                        age,
                    )
                    cur.execute(
                        """
                        UPDATE etl_metadata.loaded_partitions
                        SET dag_run_id = %s, status = 'processing', loaded_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = %s AND partition_date = %s
                        """,
                        (dag_run_id, table_name, partition_date),
                    )
                    conn.commit()
                    return True
                else:
                    raise RuntimeError(
                        f"Partition {table_name}/{partition_date} locked by another run (age {age})"
                    )
            raise RuntimeError(f"Unknown status: {status}")


def release_partition_lock(
    table_name: str,
    partition_date: str,
    success: bool,
    file_path: str | None = None,
    row_count: int = 0,
) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            if success:
                cur.execute(
                    """
                    UPDATE etl_metadata.loaded_partitions
                    SET status = 'loaded', file_path = %s, row_count = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE table_name = %s AND partition_date = %s
                    """,
                    (file_path, row_count, table_name, partition_date),
                )
            else:
                cur.execute(
                    """
                    UPDATE etl_metadata.loaded_partitions
                    SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                    WHERE table_name = %s AND partition_date = %s
                    """,
                    (table_name, partition_date),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# Приведение типов
# ---------------------------------------------------------------------------


def _cast_batch_to_schema(
    batch: pa.RecordBatch, target_schema: pa.Schema
) -> pa.RecordBatch:
    arrays = []
    for field in target_schema:
        if field.name in batch.schema.names:
            idx = batch.schema.get_field_index(field.name)
            col = batch.column(idx)
            if field.type == pa.float64() and col.type != pa.float64():
                col = col.cast(pa.float64(), safe=True)
            elif col.type != field.type:
                col = col.cast(field.type, safe=True)
        else:
            col = pa.nulls(batch.num_rows, type=field.type)
        arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


# ---------------------------------------------------------------------------
# Основная ETL-функция
# ---------------------------------------------------------------------------


def extract_load(
    table_name: str,
    cfg: Dict[str, Any],
    ds: str,
    next_ds: str,
    **context: Any,
) -> None:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    dag_run_id = dag_run.run_id if dag_run else "manual"

    fs = get_s3_filesystem()
    bucket = os.getenv("MINIO_DEFAULT_BUCKET", "datalake")

    is_fact = cfg.get("is_fact", False)
    date_col = cfg.get("date_col")
    expected_schema = EXPECTED_SCHEMAS[table_name]
    columns = TABLE_COLUMNS[table_name]

    if is_fact:
        start_str = conf.get("start_date", ds)
        end_str = conf.get("end_date", ds)
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        dates = []
        cur_date = start_date
        while cur_date <= end_date:
            dates.append(cur_date.isoformat())
            cur_date += timedelta(days=1)
    else:
        dates = ["1900-01-01"]

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    for date_str in dates:
        if is_fact:
            partition_dir = f"{bucket}/bronze/{table_name}/partition_date={date_str}"
        else:
            partition_dir = f"{bucket}/bronze/{table_name}"

        locked = False
        target_parquet_path = None
        rows_count = 0
        writer = None
        s3_file = None

        try:
            if is_fact:
                locked = acquire_partition_lock(table_name, date_str, dag_run_id)
                if not locked:
                    logger.info(
                        "Partition %s/%s already locked or loaded, skipping",
                        table_name,
                        date_str,
                    )
                    continue

            col_identifiers = [sql.Identifier(col) for col in columns]
            if is_fact and date_col:
                next_day = (
                    (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1))
                    .date()
                    .isoformat()
                )
                where_clause = sql.SQL("WHERE {} >= %s AND {} < %s").format(
                    sql.Identifier(date_col), sql.Identifier(date_col)
                )
                params = (f"{date_str} 00:00:00", f"{next_day} 00:00:00")
                order_by = sql.SQL("")
            else:
                where_clause = sql.SQL("")
                params = ()
                order_by = sql.SQL("ORDER BY {}").format(sql.Identifier(columns[0]))

            query = sql.SQL("SELECT {} FROM {} {} {}").format(
                sql.SQL(", ").join(col_identifiers),
                sql.Identifier(table_name),
                where_clause,
                order_by,
            )

            cursor_name = f"cur_{table_name}_{uuid.uuid4().hex[:8]}"

            with pg_hook.get_conn() as conn:
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

                        if writer is None:
                            file_uuid = uuid.uuid4().hex
                            parquet_filename = f"data_{file_uuid}.parquet"
                            target_parquet_path = f"{partition_dir}/{parquet_filename}"

                            if fs.exists(partition_dir):
                                try:
                                    old_files = fs.ls(partition_dir)
                                    for f in old_files:
                                        if f.endswith(".parquet"):
                                            logger.info(
                                                "Removing old parquet file to prevent duplicates: %s",
                                                f,
                                            )
                                            fs.rm(f)
                                except Exception as clean_err:
                                    logger.error(
                                        "Critical: Failed to clean up old files in %s: %s",
                                        partition_dir,
                                        clean_err,
                                    )
                                    raise RuntimeError(
                                        f"Safety cleanup failed for {partition_dir}. Aborting."
                                    ) from clean_err
                            else:
                                logger.info(
                                    "Partition directory %s does not exist. Creating.",
                                    partition_dir,
                                )
                                fs.mkdir(partition_dir)

                            s3_file = fs.open(target_parquet_path, "wb")
                            writer = pq.ParquetWriter(
                                s3_file,
                                schema=expected_schema,
                                compression="snappy",
                                use_dictionary=True,
                                data_page_size=1048576,
                                write_batch_size=100000,
                            )

                        writer.write_batch(batch)
                        rows_count += len(chunk)

            if rows_count == 0:
                logger.info("Zero rows extracted for %s/%s", table_name, date_str)
                if is_fact:
                    release_partition_lock(
                        table_name, date_str, success=True, file_path=None, row_count=0
                    )
                    locked = False
                continue

            if writer:
                writer.close()
                writer = None
            if s3_file:
                s3_file.close()
                s3_file = None

            with fs.open(target_parquet_path, "rb") as f:
                pf = pq.ParquetFile(f)
                if pf.metadata.num_rows != rows_count:
                    raise ValueError(
                        f"Integrity check failed: expected {rows_count} rows, got {pf.metadata.num_rows}"
                    )

            if is_fact:
                release_partition_lock(
                    table_name,
                    date_str,
                    success=True,
                    file_path=target_parquet_path,
                    row_count=rows_count,
                )
                locked = False

            logger.info(
                "Successfully loaded %s/%s: %d rows", table_name, date_str, rows_count
            )

        except Exception as e:
            logger.error("ETL failed for %s/%s: %s", table_name, date_str, str(e))
            if target_parquet_path and fs.exists(target_parquet_path):
                try:
                    fs.rm(target_parquet_path)
                except Exception as rm_err:
                    logger.error(
                        "Could not remove broken parquet file %s: %s",
                        target_parquet_path,
                        rm_err,
                    )

            if is_fact and locked:
                try:
                    release_partition_lock(table_name, date_str, success=False)
                except Exception as unlock_err:
                    logger.error("Failed to release lock on error: %s", unlock_err)
            raise

        finally:
            if writer:
                try:
                    writer.close()
                except Exception:
                    pass
            if s3_file:
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
    dag_id="postgres_to_minio_enterprise",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["enterprise", "etl", "minio", "data-quality"],
    max_active_runs=1,
) as dag:

    extract_tasks = []
    for tbl, cfg in TABLES_CONFIG.items():
        task = PythonOperator(
            task_id=f"extract_load_{tbl}",
            python_callable=extract_load,
            op_kwargs={
                "table_name": tbl,
                "cfg": cfg,
                "ds": "{{ ds }}",
                "next_ds": "{{ next_ds }}",
            },
        )
        extract_tasks.append(task)

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver_pipeline",
        conf="{{ dag_run.conf }}",
        wait_for_completion=False,
    )

    extract_tasks >> trigger_next
