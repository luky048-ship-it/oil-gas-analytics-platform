FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

# И ОБЯЗАТЕЛЬНО обнови ссылку на констрейнты под новую версию!
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
