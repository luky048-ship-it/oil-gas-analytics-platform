import logging
from typing import Any, Dict

import s3fs
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.s3_connection import get_polars_storage_options, get_s3_filesystem

from gold_layer.constants import POSTGRES_CONN_ID

logger = logging.getLogger(__name__)


def get_s3_fs() -> s3fs.S3FileSystem:
    """
    Возвращает готовый объект S3FileSystem с логированием процесса инициализации.
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
    Возвращает настройки Polars с проверкой на наличие ключей.
    """
    try:
        logger.debug("Retrieving Polars storage options...")
        opts = get_polars_storage_options()
        # Проверяем, что словарь не пустой (защита от кривой конфигурации)
        if not opts:
            raise ValueError("Polars storage options returned empty dictionary.")
        return opts
    except Exception as e:
        logger.error(
            f"Error retrieving Polars storage options: {str(e)}", exc_info=True
        )
        raise


def get_postgres_uri() -> str:
    """Возвращает Postgres URI для ADBC driver с логированием подключения."""
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
    """Возвращает стандартное соединение psycopg2."""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to get psycopg2 connection: {str(e)}", exc_info=True)
        raise
