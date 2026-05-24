import logging
import os
from typing import Any, Dict
from urllib.parse import quote_plus

import s3fs
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from gold_layer.constants import AWS_CONN_ID, POSTGRES_CONN_ID

logger = logging.getLogger(__name__)


def _get_aws_connection_details() -> Dict[str, Any]:
    """Извлекает параметры подключения AWS/MinIO из Airflow."""
    try:
        conn = BaseHook.get_connection(AWS_CONN_ID)
        extra = conn.extra_dejson
        return {
            "access_key": conn.login or os.getenv("MINIO_ROOT_USER", "admin"),
            "secret_key": conn.password or os.getenv("MINIO_ROOT_PASSWORD", "password"),
            "endpoint_url": extra.get(
                "endpoint_url", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
            ),
            "region": extra.get("region_name", "us-east-1"),
        }
    except Exception as e:
        logger.warning(
            f"Failed to load Airflow connection '{AWS_CONN_ID}': {e}. "
            "Falling back to local MinIO environmental defaults."
        )
        return {
            "access_key": os.getenv("MINIO_ROOT_USER", "admin"),
            "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "password"),
            "endpoint_url": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
            "region": "us-east-1",
        }


def get_s3_fs() -> s3fs.S3FileSystem:
    """Возвращает s3fs для файловых операций (поиск новых партиций через ls/glob)."""
    details = _get_aws_connection_details()
    endpoint_url = details["endpoint_url"]
    return s3fs.S3FileSystem(
        key=details["access_key"],
        secret=details["secret_key"],
        client_kwargs={
            "endpoint_url": endpoint_url,
            "region_name": details["region"],
        },
        use_ssl=False if "http://" in endpoint_url else True,
    )


def get_s3_storage_options() -> Dict[str, Any]:
    """
    Формирует словарь storage_options специально под нативный движок Polars 1.4+.
    Используется в loaders.py для scan_parquet/write_parquet.
    """
    details = _get_aws_connection_details()
    options = {
        "aws_access_key_id": details["access_key"],
        "aws_secret_access_key": details["secret_key"],
        "aws_region": details["region"],
    }
    if details["endpoint_url"]:
        options["endpoint_url"] = details["endpoint_url"]
        if "http://" in details["endpoint_url"]:
            options["aws_allow_http"] = True
    return options


def get_postgres_uri() -> str:
    """Returns Postgres URI string required for Polars DB read/write via ADBC."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_connection(POSTGRES_CONN_ID)
    extra = conn.extra_dejson
    dbname = extra.get("dbname", conn.schema or "postgres")
    user = quote_plus(conn.login or "")
    password = quote_plus(conn.password or "")
    host = conn.host or "localhost"
    port = conn.port or 5432
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def get_psycopg2_conn():
    """Returns standard psycopg2 connection for synchronous metadata transactions."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()
