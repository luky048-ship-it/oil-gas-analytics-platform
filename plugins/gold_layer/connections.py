import logging
from typing import Any, Dict

import s3fs
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.s3_connection import get_polars_storage_options, get_s3_filesystem

from gold_layer.constants import POSTGRES_CONN_ID

logger = logging.getLogger(__name__)


def get_s3_fs() -> s3fs.S3FileSystem:
    """
    Инициализирует и возвращает объект файловой системы S3 (fsspec).
    Централизованная точка доступа к хранилищу для Gold слоя.
    """
    try:
        logger.info("Attempting to initialize S3FileSystem...")
        fs = get_s3_filesystem()
        logger.info("S3FileSystem successfully initialized.")
        return fs
    except Exception as e:
        logger.error(f"Failed to initialize S3FileSystem: {str(e)}", exc_info=True)
        raise


def get_s3_polars_opts() -> Dict[str, Any]:
    """
    Возвращает настройки конфигурации хранилища, оптимизированные для использования в Polars.
    Включает проверку корректности полученных параметров.
    """
    try:
        logger.debug("Retrieving Polars storage options...")
        opts = get_polars_storage_options()
        if not opts:
            raise ValueError("Polars storage options returned empty dictionary.")
        return opts
    except Exception as e:
        logger.error(
            f"Error retrieving Polars storage options: {str(e)}", exc_info=True
        )
        raise


def get_postgres_uri() -> str:
    """
    Формирует и возвращает строку подключения (URI) к Postgres для драйверов ADBC/SQLAlchemy.
    Извлекает параметры авторизации из Airflow Connections.
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_connection(POSTGRES_CONN_ID)
        logger.debug(f"Postgres connection {POSTGRES_CONN_ID} retrieved successfully.")
        return f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    except Exception as e:
        logger.error(
            f"Failed to build Postgres URI for {POSTGRES_CONN_ID}: {str(e)}",
            exc_info=True,
        )
        raise


def get_psycopg2_conn():
    """
    Инициализирует стандартное соединение с Postgres через библиотеку psycopg2.
    Используется для выполнения транзакционных SQL-запросов и DDL операций.
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to get psycopg2 connection: {str(e)}", exc_info=True)
        raise
