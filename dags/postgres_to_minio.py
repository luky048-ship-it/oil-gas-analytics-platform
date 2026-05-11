from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Any

import pandas as pd
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

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

# Параметры подключения к MinIO (берутся из .env или переменных окружения)
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
S3_BUCKET = os.getenv("MINIO_DEFAULT_BUCKET", "datalake")

# Словарь доступов для pandas
STORAGE_OPTIONS = {
    "key": S3_ACCESS_KEY,
    "secret": S3_SECRET_KEY,
    "client_kwargs": {"endpoint_url": S3_ENDPOINT},
}


def extract_load(table_name: str, cfg: Dict[str, Any], ds: str, **kwargs) -> None:
    """Выгрузить данные из PostgreSQL и записать их в MinIO."""

    # 1. Формируем пути и проверяем идемпотентность
    fs = s3fs.S3FileSystem(**STORAGE_OPTIONS)

    if cfg["is_fact"]:

        check_path = f"{S3_BUCKET}/raw/{table_name}/partition_date={ds}"
    else:

        check_path = f"{S3_BUCKET}/raw/{table_name}/{table_name}.parquet"

    if fs.exists(check_path):
        print(f"Данные по пути {check_path} уже существуют. Пропуск (Skip).")
        return

    # 2. Подключаемся к PostgreSQL. Приоритет: Airflow connection, затем переменные .env
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

    # 3. Формируем SQL-запрос и читаем данные
    if cfg["is_fact"]:
        sql = f"SELECT * FROM {table_name} WHERE DATE({cfg['date_col']}) = '{ds}'"
    else:
        sql = f"SELECT * FROM {table_name}"

    print(f"Выполнение запроса: {sql}")
    df = pd.read_sql(sql, engine)

    if df.empty:
        print(f"Нет данных для выгрузки (таблица {table_name}, дата {ds}).")
        return

    # 4. Сохраняем в MinIO
    base_s3_uri = f"s3://{S3_BUCKET}/raw/{table_name}"

    if cfg["is_fact"]:

        df[cfg["date_col"]] = pd.to_datetime(df[cfg["date_col"]])
        df["partition_date"] = df[cfg["date_col"]].dt.date

        df.to_parquet(
            base_s3_uri,
            engine="pyarrow",
            compression="snappy",
            partition_cols=["partition_date"],
            storage_options=STORAGE_OPTIONS,
            index=False,
        )
    else:

        file_s3_uri = f"{base_s3_uri}/{table_name}.parquet"
        df.to_parquet(
            file_s3_uri,
            engine="pyarrow",
            compression="snappy",
            storage_options=STORAGE_OPTIONS,
            index=False,
        )

    print(f"Таблица {table_name} успешно выгружена!")


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
    start_date=datetime(2025, 10, 1),
    catchup=True,
    tags=["hw", "etl", "minio"],
    max_active_runs=1,
) as dag:

    for tbl, cfg in TABLES_CONFIG.items():
        PythonOperator(
            task_id=f"extract_load_{tbl}",
            python_callable=extract_load,
            op_kwargs={"table_name": tbl, "cfg": cfg},
        )
