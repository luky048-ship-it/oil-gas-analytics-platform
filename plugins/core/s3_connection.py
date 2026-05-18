import logging
from typing import Any, Dict

import s3fs
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def get_polars_storage_options(conn_id: str = "aws_default") -> Dict[str, Any]:
    """
    Возвращает параметры конфигурации для библиотеки Polars (Rust ObjectStore) на основе Airflow Connection.
    Используется для прямого доступа к S3/MinIO из Polars.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

        logger.info(
            f"Generating Polars storage_options using connection: {conn_id} at {endpoint}"
        )

        return {
            "aws_access_key_id": conn.login,
            "aws_secret_access_key": conn.password,
            "aws_endpoint_url": endpoint,
            "aws_region": "us-east-1",
            "aws_allow_http": "true",
        }
    except Exception as e:
        logger.error(
            f"Error fetching connection '{conn_id}' for Polars: {str(e)}", exc_info=True
        )
        raise


def get_s3_filesystem(conn_id: str = "aws_default") -> s3fs.S3FileSystem:
    """
    Инициализирует и возвращает объект S3FileSystem (fsspec) для работы с файлами в облачном хранилище.
    Инкапсулирует настройки авторизации и эндпоинтов.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

        # Определение использования защищенного протокола SSL
        use_ssl = not endpoint.startswith("http://")

        logger.info(f"Initializing s3fs.S3FileSystem for {endpoint} (SSL: {use_ssl})")

        return s3fs.S3FileSystem(
            key=conn.login,
            secret=conn.password,
            client_kwargs={"endpoint_url": endpoint},
            use_ssl=use_ssl,
        )
    except Exception as e:
        logger.error(
            f"Failed to initialize S3FileSystem for connection '{conn_id}': {str(e)}",
            exc_info=True,
        )
        raise
