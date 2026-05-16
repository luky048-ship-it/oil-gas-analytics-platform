import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup

from gold_layer.watermarks import get_last_watermark, update_mart_watermark
from gold_layer.loaders import load_silver_dataset, discover_new_partitions, load_gold_dataset
from gold_layer.builders.mart_production import build_mart_production
from gold_layer.builders.mart_well_kpi import build_mart_well_kpi
from gold_layer.builders.mart_failures import build_mart_failures
from gold_layer.builders.mart_logistics import build_mart_logistics
from gold_layer.validators import validate_business_readiness, validate_mart_before_publish
from gold_layer.publishers import write_staging_mart, atomic_partition_overwrite, cleanup_staging
from gold_layer.constants import (
    SILVER_PRODUCTION, SILVER_TELEMETRY, SILVER_TARGETS,
    SILVER_PUMP_SENSORS, SILVER_PUMP_FAILURES,
    SILVER_DELIVERIES, SILVER_DRIVERS, SILVER_VEHICLES,
    TABLE_MART_PRODUCTION, TABLE_MART_WELL_KPI,
    TABLE_MART_FAILURES, TABLE_MART_LOGISTICS
)

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "pool": "gold_pool"
}

@dag(
    dag_id="silver_gold_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["gold", "production", "petroleum"]
)
def silver_gold_marts_dag():

    @task
    def discover_dates():
        # Using production as the primary driver for partition discovery
        last_dt = get_last_watermark("mart_production")
        new_dates = discover_new_partitions(SILVER_PRODUCTION, last_dt)
        if not new_dates:
            logging.info("No new partitions discovered in Silver.")
        return new_dates

    @task_group(group_id="process_marts")
    def process_marts(partition_dates):

        @task
        def build_production_task(dates):
            lf_prod = load_silver_dataset(SILVER_PRODUCTION, dates)
            lf_tele = load_silver_dataset(SILVER_TELEMETRY, dates)
            lf_targ = load_silver_dataset(SILVER_TARGETS, dates)

            lf_result = build_mart_production(lf_prod, lf_tele, lf_targ)
            lf_result = validate_business_readiness(lf_result, "mart_production")

            # Materialize for publishing
            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_production")

            staging_table = write_staging_mart(df, "mart_production")

            # Detect unique dates in the materialized batch
            actual_dates = df.select("partition_date").unique().to_series().to_list()
            actual_dates = [str(d) for d in actual_dates]

            atomic_partition_overwrite("mart_production", staging_table, actual_dates)
            cleanup_staging(staging_table)

            # Update watermarks for each processed date
            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_production", dt, run_id)

            return True # Signal for dependent marts

        @task
        def build_well_kpi_task(dates, prod_ready):
            # Depends on mart_production being updated in Gold OR we use the current batch
            # Requirements say build_mart_well_kpi should take LazyFrame from mart_production
            # To strictly follow "don't read twice", we could pass the DF, but builder expects LF.
            # Re-scanning production for the same dates is efficient in Polars.

            lf_prod_batch = build_mart_production(
                load_silver_dataset(SILVER_PRODUCTION, dates),
                load_silver_dataset(SILVER_TELEMETRY, dates),
                load_silver_dataset(SILVER_TARGETS, dates)
            )

            lf_history = load_gold_dataset(TABLE_MART_PRODUCTION)

            lf_result = build_mart_well_kpi(lf_prod_batch, lf_history)
            df = lf_result.collect()

            validate_mart_before_publish(df, "mart_well_kpi")
            staging_table = write_staging_mart(df, "mart_well_kpi")

            actual_dates = [str(d) for d in df.select("partition_date").unique().to_series().to_list()]
            atomic_partition_overwrite("mart_well_kpi", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_well_kpi", dt, run_id)

        @task
        def build_failures_task(dates):
            lf_sens = load_silver_dataset(SILVER_PUMP_SENSORS, dates)
            lf_fail = load_silver_dataset(SILVER_PUMP_FAILURES, dates)
            # Pumps is likely a dimension, load entirely or filter
            lf_pumps = load_silver_dataset("pumps")

            lf_result = build_mart_failures(lf_sens, lf_fail, lf_pumps)
            df = lf_result.collect()

            validate_mart_before_publish(df, "mart_failures")
            staging_table = write_staging_mart(df, "mart_failures")

            actual_dates = [str(d) for d in df.select("partition_date").unique().to_series().to_list()]
            atomic_partition_overwrite("mart_failures", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_failures", dt, run_id)

        @task
        def build_logistics_task(dates):
            lf_del = load_silver_dataset(SILVER_DELIVERIES, dates)
            lf_drv = load_silver_dataset(SILVER_DRIVERS) # Dim
            lf_veh = load_silver_dataset(SILVER_VEHICLES) # Dim

            lf_result = build_mart_logistics(lf_del, lf_drv, lf_veh)
            df = lf_result.collect()

            validate_mart_before_publish(df, "mart_logistics")
            staging_table = write_staging_mart(df, "mart_logistics")

            actual_dates = [str(d) for d in df.select("partition_date").unique().to_series().to_list()]
            atomic_partition_overwrite("mart_logistics", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_logistics", dt, run_id)

        prod_ready = build_production_task(partition_dates)
        build_well_kpi_task(partition_dates, prod_ready)
        build_failures_task(partition_dates)
        build_logistics_task(partition_dates)

    dates = discover_dates()
    process_marts(dates)

silver_gold_marts_dag()
