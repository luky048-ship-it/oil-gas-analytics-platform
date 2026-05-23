FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Основные зависимости Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

# Постоянный venv для ML
RUN python -m venv /opt/airflow/ml_scripts/venv
COPY ml_scripts/requirements.txt /opt/airflow/ml_scripts/requirements.txt
RUN /opt/airflow/ml_scripts/venv/bin/pip install --no-cache-dir \
    -r /opt/airflow/ml_scripts/requirements.txt

# Код ML-скриптов
COPY ml_scripts/ /opt/airflow/ml_scripts/

# Директория для сохранения обученных моделей
RUN mkdir -p /opt/airflow/ml_models && chown airflow: /opt/airflow/ml_models
