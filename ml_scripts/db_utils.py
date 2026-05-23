import os
from urllib.parse import quote_plus


def get_postgres_uri() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    dbname = os.environ.get("POSTGRES_DB", "postgres")
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{dbname}"
    )


def get_psycopg2_conn():
    import psycopg2

    return psycopg2.connect(get_postgres_uri())


def get_s3_storage_options() -> dict:
    return {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "aws_endpoint_url": os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
        "aws_allow_http": os.environ.get("AWS_ALLOW_HTTP", "true"),
    }
