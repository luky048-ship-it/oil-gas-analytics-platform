from __future__ import annotations

import io
import logging
import os
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


# ---------------------------------------------------------------------------
# Методы для работы с мета‑таблицей loaded_partitions
# ---------------------------------------------------------------------------
def is_partition_loaded(table_name: str, ds: str) -> bool:
    """Проверить, загружалась ли уже партиция ``ds`` для ``table_name``.

    Возвращает ``True`` если запись существует в ``etl_metadata.loaded_partitions``.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM etl_metadata.loaded_partitions "
                "WHERE table_name=%s AND partition_date=%s",
                (table_name, ds),
            )
            return cur.fetchone() is not None


def mark_partition_loaded(table_name: str, ds: str, dag_run_id: str) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_metadata.loaded_partitions
                (table_name, partition_date, dag_run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (table_name, ds, dag_run_id),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Конфигурация таблиц
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# MinIO connection handling
# ---------------------------------------------------------------------------


def _get_minio_credentials() -> Dict[str, str]:
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT_URL")

    if not (access_key and secret_key and endpoint):
        hook = S3Hook(aws_conn_id="minio_default")
        conn = hook.get_connection("minio_default")
        access_key = access_key or conn.login
        secret_key = secret_key or conn.password
        endpoint = endpoint or conn.extra_dejson.get(
            "endpoint_url", "http://minio:9000"
        )

    if not (access_key and secret_key and endpoint):
        raise ValueError(
            "MinIO credentials are incomplete. Check env vars or Airflow connection."
        )

    return {"access_key": access_key, "secret_key": secret_key, "endpoint": endpoint}


S3_BUCKET = os.getenv("MINIO_DEFAULT_BUCKET", "datalake")


def extract_load(table_name: str, cfg: Dict[str, Any], ds: str, **context: Any) -> None:
    dag_run_id: str = context["dag_run"].run_id

    # 1. Skip already loaded partitions for fact tables
    if cfg["is_fact"] and is_partition_loaded(table_name, ds):
        logging.info("Данные за %s для %s уже загружены. Пропускаем.", ds, table_name)
        return

    # 2. Resolve MinIO credentials and build storage options
    creds = _get_minio_credentials()
    storage_options = {
        "key": creds["access_key"],
        "secret": creds["secret_key"],
        "client_kwargs": {"endpoint_url": creds["endpoint"]},
    }
    fs = s3fs.S3FileSystem(**storage_options)

    # 3. Connect to PostgreSQL (fallback to DSN from env if hook fails)
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()
    except Exception:
        pg_user = os.getenv("POSTGRES_USER", "admin")
        pg_password = os.getenv("POSTGRES_PASSWORD", "password")
        pg_db = os.getenv("POSTGRES_DB", "postgres")
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        dsn = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(dsn)

    # 4. Build SQL and load data
    sql = (
        f"SELECT * FROM {table_name} WHERE DATE({cfg['date_col']}) = '{ds}'"
        if cfg["is_fact"]
        else f"SELECT * FROM {table_name}"
    )
    logging.info("Выполнение запроса: %s", sql)
    df = pd.read_sql(sql, engine)

    if df.empty:
        logging.warning(
            "Нет данных для выгрузки (таблица %s, дата %s).", table_name, ds
        )
        return

    # 5. Prepare Parquet buffer
    parquet_buffer = io.BytesIO()
    if cfg["is_fact"]:
        df[cfg["date_col"]] = pd.to_datetime(df[cfg["date_col"]])
        df["partition_date"] = df[cfg["date_col"]].dt.date
        df.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="snappy",
            partition_cols=["partition_date"],
            index=False,
        )
        # Construct path with partition placeholder – s3fs will handle directories
        s3_path = f"s3://{S3_BUCKET}/raw/{table_name}/partition_date={ds}/data.parquet"
    else:
        df.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        s3_path = f"s3://{S3_BUCKET}/raw/{table_name}/{table_name}.parquet"

    parquet_buffer.seek(0)
    # 6. Write to MinIO
    with fs.open(s3_path, "wb") as f:
        f.write(parquet_buffer.read())

    logging.info("Таблица %s успешно выгружена!", table_name)

    # 7. Mark partition as loaded for fact tables
    if cfg["is_fact"]:
        mark_partition_loaded(table_name, ds, dag_run_id)


# ---------------------------------------------------------------------------
# Определение DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="postgres_to_minio_etl",
    default_args=default_args,
    description="Инкрементальная выгрузка данных из PostgreSQL в MinIO",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["hw", "etl", "minio"],
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
            },
            provide_context=True,
        )
