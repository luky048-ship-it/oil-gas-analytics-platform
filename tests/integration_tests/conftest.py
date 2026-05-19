"""
Pytest conftest.py для integration_tests.

Этот файл автоматически поднимает MinIO и Postgres через testcontainers,
создаёт бакеты/таблицы (из init-sql/*.sql) и подчищает всё после тестов.
"""

import os
import time
from pathlib import Path
from typing import Generator, Optional

import boto3
import pytest
import psycopg2
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

MINIO_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1"
POSTGRES_IMAGE = "postgres:15.6-alpine"

MINIO_ROOT_USER = "test_minio_admin"
MINIO_ROOT_PASSWORD = "test_minio_password"
MINIO_DEFAULT_BUCKET = "datalake"

POSTGRES_USER = "test_user"
POSTGRES_PASSWORD = "test_password"
POSTGRES_DB = "test_db"

INIT_SQL_DIR = Path(__file__).parent.parent.parent / "init-sql"


# =============================================================================
# FIXTURES ДЛЯ MINIO
# =============================================================================


@pytest.fixture(scope="session")
def minio_container() -> Generator[DockerContainer, None, None]:
    """
    Запускает контейнер MinIO через testcontainers.
    
    Yields:
        DockerContainer: запущенный контейнер MinIO
    """
    container = (
        DockerContainer(MINIO_IMAGE)
        .with_name("integration_test_minio")
        .with_command("server /data --console-address ':9001'")
        .with_env("MINIO_ROOT_USER", MINIO_ROOT_USER)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
        .with_exposed_ports(9000, 9001)
    )
    
    container.start()
    wait_for_logs(container, "API:", timeout=60)
    
    yield container
    
    container.stop()


@pytest.fixture(scope="session")
def minio_endpoint(minio_container: DockerContainer) -> str:
    """
    Возвращает endpoint URL для подключения к MinIO.
    
    Args:
        minio_container: контейнер MinIO
        
    Returns:
        str: endpoint URL вида http://localhost:PORT
    """
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def s3_client(minio_endpoint: str) -> boto3.client:
    """
    Создаёт S3-клиент для работы с MinIO.
    
    Args:
        minio_endpoint: endpoint URL MinIO
        
    Returns:
        boto3.client: S3-клиент
    """
    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1",
    )
    return client


@pytest.fixture(scope="session")
def s3_resource(minio_endpoint: str) -> boto3.resource:
    """
    Создаёт S3-ресурс для работы с MinIO.
    
    Args:
        minio_endpoint: endpoint URL MinIO
        
    Returns:
        boto3.resource: S3-ресурс
    """
    resource = boto3.resource(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1",
    )
    return resource


@pytest.fixture(scope="function")
def minio_bucket(s3_client: boto3.client) -> Generator[str, None, None]:
    """
    Создаёт временный бакет для тестов и удаляет его после завершения.
    
    Args:
        s3_client: S3-клиент
        
    Yields:
        str: имя созданного бакета
    """
    bucket_name = f"{MINIO_DEFAULT_BUCKET}-test-{int(time.time())}"
    
    # Создаём бакет
    s3_client.create_bucket(Bucket=bucket_name)
    
    # Ждём пока бакет станет доступен
    waiter = s3_client.get_waiter("bucket_exists")
    waiter.wait(Bucket=bucket_name)
    
    yield bucket_name
    
    # Очистка: удаляем все объекты и сам бакет
    try:
        objects_to_delete = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in objects_to_delete:
            delete_keys = [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": delete_keys}
            )
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Warning: Failed to clean up bucket {bucket_name}: {e}")


@pytest.fixture(scope="function")
def minio_buckets(s3_client: boto3.client) -> Generator[dict, None, None]:
    """
    Создаёт стандартную структуру бакетов для ETL-пайплайна:
    - datalake/raw (bronze layer)
    - datalake/silver
    - datalake/quarantine
    
    Args:
        s3_client: S3-клиент
        
    Yields:
        dict: словарь с именами бакетов и префиксами
    """
    base_bucket = f"{MINIO_DEFAULT_BUCKET}-test-{int(time.time())}"
    
    # Создаём основной бакет
    s3_client.create_bucket(Bucket=base_bucket)
    
    # Префиксы для слоёв
    prefixes = {
        "raw": "raw",
        "silver": "silver",
        "quarantine": "quarantine",
        "gold": "gold",
    }
    
    yield {
        "bucket": base_bucket,
        "raw": f"s3://{base_bucket}/{prefixes['raw']}",
        "silver": f"s3://{base_bucket}/{prefixes['silver']}",
        "quarantine": f"s3://{base_bucket}/{prefixes['quarantine']}",
        "gold": f"s3://{base_bucket}/{prefixes['gold']}",
        "prefixes": prefixes,
    }
    
    # Очистка
    try:
        objects_to_delete = s3_client.list_objects_v2(Bucket=base_bucket)
        if "Contents" in objects_to_delete:
            delete_keys = [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]
            s3_client.delete_objects(
                Bucket=base_bucket,
                Delete={"Objects": delete_keys}
            )
        s3_client.delete_bucket(Bucket=base_bucket)
    except Exception as e:
        print(f"Warning: Failed to clean up bucket {base_bucket}: {e}")


# =============================================================================
# FIXTURES ДЛЯ POSTGRESQL
# =============================================================================


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Запускает контейнер PostgreSQL через testcontainers.
    
    Yields:
        PostgresContainer: запущенный контейнер PostgreSQL
    """
    container = (
        PostgresContainer(POSTGRES_IMAGE)
        .with_name("integration_test_postgres")
        .with_env("POSTGRES_USER", POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env("POSTGRES_DB", POSTGRES_DB)
    )
    
    container.start()
    wait_for_logs(container, "database system is ready to accept connections", timeout=60)
    
    yield container
    
    container.stop()


@pytest.fixture(scope="session")
def postgres_connection_url(postgres_container: PostgresContainer) -> str:
    """
    Возвращает connection URL для подключения к PostgreSQL.
    
    Args:
        postgres_container: контейнер PostgreSQL
        
    Returns:
        str: connection URL
    """
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
def postgres_conn(postgres_container: PostgresContainer) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Создаёт psycopg2-соединение с PostgreSQL.
    
    Args:
        postgres_container: контейнер PostgreSQL
        
    Yields:
        psycopg2.extensions.connection: активное соединение
    """
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    conn.autocommit = False
    
    yield conn
    
    conn.close()


@pytest.fixture(scope="function")
def postgres_initialized(postgres_conn: psycopg2.extensions.connection) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Инициализирует базу данных SQL-скриптами из init-sql/*.sql.
    
    Выполняет все *.sql файлы в алфавитном порядке.
    После тестов откатывает все изменения (транзакция rollback).
    
    Args:
        postgres_conn: psycopg2-соединение
        
    Yields:
        psycopg2.extensions.connection: соединение с инициализированной БД
    """
    cursor = postgres_conn.cursor()
    
    # Получаем список SQL-файлов
    sql_files = sorted(INIT_SQL_DIR.glob("*.sql"))
    
    if not sql_files:
        print(f"Warning: No SQL files found in {INIT_SQL_DIR}")
        yield postgres_conn
        return
    
    # Выполняем каждый SQL-файл
    for sql_file in sql_files:
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_script = f.read()
            
            cursor.execute(sql_script)
            print(f"Executed: {sql_file.name}")
        except Exception as e:
            print(f"Error executing {sql_file.name}: {e}")
            raise
    
    # Сохраняем изменения
    postgres_conn.commit()
    
    yield postgres_conn
    
    # Откатываем все изменения после теста (clean up)
    try:
        postgres_conn.rollback()
        
        # Дополнительно: дропаем схемы если они были созданы в тестах
        cursor.execute("""
            DROP SCHEMA IF EXISTS staging CASCADE;
            DROP SCHEMA IF EXISTS gold CASCADE;
            DROP SCHEMA IF EXISTS etl_metadata CASCADE;
        """)
        postgres_conn.commit()
    except Exception as e:
        print(f"Warning: Failed to rollback changes: {e}")
    
    cursor.close()


@pytest.fixture(scope="function")
def postgres_tables(postgres_initialized: psycopg2.extensions.connection) -> dict:
    """
    Возвращает словарь с именами таблиц, созданных из init-sql.
    
    Args:
        postgres_initialized: соединение с инициализированной БД
        
    Returns:
        dict: {table_name: schema_name}
    """
    cursor = postgres_initialized.cursor()
    
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """)
    
    tables = {}
    for schema, table in cursor.fetchall():
        tables[table] = schema
    
    cursor.close()
    return tables


# =============================================================================
# КОМБИНИРОВАННЫЕ FIXTURES
# =============================================================================


@pytest.fixture(scope="session")
def integration_environment(
    minio_container: DockerContainer,
    minio_endpoint: str,
    postgres_container: PostgresContainer,
    postgres_connection_url: str,
) -> dict:
    """
    Создаёт полное интеграционное окружение: MinIO + PostgreSQL.
    
    Этот fixture удобно использовать когда нужны оба сервиса одновременно.
    
    Yields:
        dict: конфигурация окружения
    """
    env = {
        "minio": {
            "endpoint": minio_endpoint,
            "access_key": MINIO_ROOT_USER,
            "secret_key": MINIO_ROOT_PASSWORD,
            "bucket": MINIO_DEFAULT_BUCKET,
        },
        "postgres": {
            "connection_url": postgres_connection_url,
            "host": postgres_container.get_container_host_ip(),
            "port": postgres_container.get_exposed_port(5432),
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "dbname": POSTGRES_DB,
        },
    }
    
    # Устанавливаем переменные окружения для совместимости с кодом
    os.environ["MINIO_ENDPOINT"] = minio_endpoint
    os.environ["MINIO_ACCESS_KEY"] = MINIO_ROOT_USER
    os.environ["MINIO_SECRET_KEY"] = MINIO_ROOT_PASSWORD
    os.environ["POSTGRES_CONNECTION_URL"] = postgres_connection_url
    
    yield env
    
    # Cleanup env vars
    os.environ.pop("MINIO_ENDPOINT", None)
    os.environ.pop("MINIO_ACCESS_KEY", None)
    os.environ.pop("MINIO_SECRET_KEY", None)
    os.environ.pop("POSTGRES_CONNECTION_URL", None)


# =============================================================================
# HELPER FIXTURES ДЛЯ POLARS + S3
# =============================================================================


@pytest.fixture(scope="function")
def polars_storage_options(minio_endpoint: str) -> dict:
    """
    Возвращает storage_options для Polars при работе с MinIO.
    
    Args:
        minio_endpoint: endpoint URL MinIO
        
    Returns:
        dict: storage_options для pl.scan_parquet() / pl.write_parquet()
    """
    return {
        "aws_access_key_id": MINIO_ROOT_USER,
        "aws_secret_access_key": MINIO_ROOT_PASSWORD,
        "aws_endpoint_url": minio_endpoint,
        "aws_region": "us-east-1",
        "aws_allow_http": "true",
    }


@pytest.fixture(scope="function")
def s3fs_filesystem(minio_endpoint: str):
    """
    Создаёт s3fs.S3FileSystem для работы с MinIO.
    
    Args:
        minio_endpoint: endpoint URL MinIO
        
    Returns:
        s3fs.S3FileSystem: файловая система
    """
    import s3fs
    
    fs = s3fs.S3FileSystem(
        key=MINIO_ROOT_USER,
        secret=MINIO_ROOT_PASSWORD,
        client_kwargs={"endpoint_url": minio_endpoint},
        use_ssl=False,
    )
    return fs


# =============================================================================
# PYTEST HOOKS
# =============================================================================


def pytest_configure(config):
    """
    Настраивает дополнительные маркеры для тестов.
    """
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (deselect with '-m \"not integration\"')",
    )
    config.addinivalue_line(
        "markers",
        "requires_minio: marks tests as requiring MinIO",
    )
    config.addinivalue_line(
        "markers",
        "requires_postgres: marks tests as requiring PostgreSQL",
    )
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow running",
    )
