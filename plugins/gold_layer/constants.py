# /plugins/gold_layer/constants.py
import os

# Connections
POSTGRES_CONN_ID = "postgres_default"
AWS_CONN_ID = "aws_default"

# S3 Paths
S3_BUCKET = os.getenv("MINIO_DEFAULT_BUCKET", "datalake")
SILVER_PREFIX = f"s3://{S3_BUCKET}/silver"

# Tables
GOLD_SCHEMA = "gold"
STAGING_SCHEMA = "staging"
METADATA_SCHEMA = "etl_metadata"

TABLE_MART_PRODUCTION = f"{GOLD_SCHEMA}.mart_production"
TABLE_MART_WELL_KPI = f"{GOLD_SCHEMA}.mart_well_kpi"
TABLE_MART_FAILURES = f"{GOLD_SCHEMA}.mart_failures"
TABLE_MART_LOGISTICS = f"{GOLD_SCHEMA}.mart_logistics"

TABLE_METADATA_WATERMARKS = f"{METADATA_SCHEMA}.marts_loaded_partitions"

# Silver Datasets
SILVER_PRODUCTION = "production"
SILVER_TELEMETRY = "well_telemetry"
SILVER_TARGETS = "well_targets"
SILVER_PUMP_SENSORS = "pump_sensors"
SILVER_PUMP_FAILURES = "pump_failures"
SILVER_DELIVERIES = "deliveries"
SILVER_DRIVERS = "drivers"
SILVER_VEHICLES = "vehicles"

# Business Constants
Z_SCORE_THRESHOLD = 3.0
RISK_WINDOW_DAYS = 7
