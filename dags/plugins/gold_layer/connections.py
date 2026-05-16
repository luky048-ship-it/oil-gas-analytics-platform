import os
import s3fs
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from dags.plugins.gold_layer.constants import POSTGRES_CONN_ID, AWS_CONN_ID

def get_s3_fs() -> s3fs.S3FileSystem:
    """Returns a configured s3fs filesystem using Airflow AWS connection."""
    aws_hook = AwsGenericHook(aws_conn_id=AWS_CONN_ID, client_type="s3")
    creds = aws_hook.get_credentials()

    # Check if we are using MinIO (standard for this project based on README/previous logs)
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")

    return s3fs.S3FileSystem(
        key=creds.access_key,
        secret=creds.secret_key,
        token=creds.token,
        client_kwargs={'endpoint_url': endpoint_url},
        use_ssl=False if "http://" in endpoint_url else True
    )

def get_postgres_uri() -> str:
    """Returns Postgres URI for ADBC driver."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_connection(POSTGRES_CONN_ID)
    return f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

def get_psycopg2_conn():
    """Returns a standard psycopg2 connection."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()
