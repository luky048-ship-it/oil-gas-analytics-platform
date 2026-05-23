import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "ml_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_inference",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["ml", "inference", "docker"],
) as dag:

    @task
    def get_dates(**context) -> list:
        conf = context["dag_run"].conf
        if conf and "dates" in conf:
            return conf["dates"]
        if conf and "start_date" in conf and "end_date" in conf:
            start = datetime.strptime(conf["start_date"], "%Y-%m-%d").date()
            end = datetime.strptime(conf["end_date"], "%Y-%m-%d").date()
            dates = []
            cur = start
            while cur <= end:
                dates.append(cur.isoformat())
                cur += timedelta(days=1)
            return dates
        return [context["ds"]]

    @task
    def build_env(dates: list) -> dict:
        return {
            "TARGET_DATES_JSON": json.dumps(dates),
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

    dates = get_dates()
    env_vars = build_env(dates)

    predict_flow = DockerOperator(
        task_id="predict_flow",
        image="ml-scripts:latest",
        command="python /opt/ml/predict_flow.py",
        environment=env_vars,
        network_mode="bridge",
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    predict_pump = DockerOperator(
        task_id="predict_pump",
        image="ml-scripts:latest",
        command="python /opt/ml/predict_failures.py",
        environment=env_vars,
        network_mode="bridge",
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    finish = EmptyOperator(task_id="finish")
    [predict_flow, predict_pump] >> finish
