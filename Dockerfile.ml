FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ml

COPY ml_scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ml_scripts/ /opt/ml/
