from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "ml_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ml_training",
    schedule_interval="0 2 * * 0",  # каждое воскресенье в 02:00
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["ml", "training", "docker"],
) as dag:

    env_vars = {
        "POSTGRES_USER": "{{ conn.postgres_default.login }}",
        "POSTGRES_PASSWORD": "{{ conn.postgres_default.password }}",
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "{{ conn.postgres_default.schema }}",
        "AWS_ACCESS_KEY_ID": "{{ conn.aws_default.login }}",
        "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_default.password }}",
        "AWS_ENDPOINT_URL": "{{ conn.aws_default.extra_dejson.endpoint_url }}",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }

    train_flow = DockerOperator(
        task_id="train_flow_model",
        image="ml-scripts:latest",
        command="python /opt/ml/train_flow_model.py --days-back 365",
        environment=env_vars,
        network_mode="bridge",
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    train_pump = DockerOperator(
        task_id="train_pump_model",
        image="ml-scripts:latest",
        command="python /opt/ml/train_pump_model.py --days-back 90",
        environment=env_vars,
        network_mode="bridge",
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    train_flow >> train_pump
