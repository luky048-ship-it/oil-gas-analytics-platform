import logging
from datetime import datetime, timedelta

import polars as pl
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import MART_CONTRACTS
from gold_layer.builders.mart_failures import build_mart_failures
from gold_layer.builders.mart_logistics import build_mart_logistics
from gold_layer.builders.mart_production import build_mart_production
from gold_layer.builders.mart_well_kpi import build_mart_well_kpi
from gold_layer.constants import (SILVER_DELIVERIES, SILVER_DRIVERS,
                                  SILVER_PRODUCTION, SILVER_PUMP_FAILURES,
                                  SILVER_PUMP_SENSORS, SILVER_TARGETS,
                                  SILVER_TELEMETRY, SILVER_VEHICLES,
                                  TABLE_MART_FAILURES, TABLE_MART_LOGISTICS,
                                  TABLE_MART_PRODUCTION, TABLE_MART_WELL_KPI)
from gold_layer.loaders import (discover_new_partitions, load_gold_dataset,
                                load_silver_dataset)
from gold_layer.publishers import (atomic_partition_overwrite, cleanup_staging,
                                   write_staging_mart)
from gold_layer.validators import (validate_business_readiness,
                                   validate_mart_before_publish)
from gold_layer.watermarks import get_last_watermark, update_mart_watermark

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "pool": "gold_pool",
}


@dag(
    dag_id="silver_gold_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["gold", "production", "petroleum"],
)
def silver_gold_marts_dag():

    def _check_dq_status(dataset_name: str, dates: list[str]) -> bool:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                                SELECT DISTINCT status 
                                FROM etl_metadata.dq_pipeline_runs
                                WHERE dataset = %s AND partition_date = ANY(%s)
                            """,
                    (dataset_name, dates),
                )
                rows = cur.fetchall()
        if not rows:
            logging.warning(f"No DQ status for {dataset_name} on {dates}. Blocking.")
            return False
        return all(row[0] == "SUCCESS" for row in rows)

    @task
    def discover_dates():
        last_dt = get_last_watermark("mart_production")
        new_dates = discover_new_partitions(SILVER_PRODUCTION, last_dt)
        if not new_dates:
            raise AirflowSkipException("No new DQ-validated partitions in Silver.")
        return new_dates

    @task_group(group_id="process_marts")
    def process_marts(partition_dates):

        @task
        def build_production_task(dates):
            contract = MART_CONTRACTS["mart_production"]
            if not _check_dq_status("production", dates):
                raise AirflowSkipException(f"DQ != SUCCESS for production on {dates}")

            sources = {
                name: load_silver_dataset(name, dates, pk_columns=pks)
                for name, pks in contract.source_datasets.items()
            }

            lf_result = build_mart_production(
                sources["production"],
                sources["well_telemetry"],
                sources["well_targets"],
            )
            lf_result = validate_business_readiness(lf_result, "mart_production")

            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_production")
            staging_table = write_staging_mart(df, "mart_production")

            actual_dates = [
                str(d)
                for d in df.select("partition_date").unique().to_series().to_list()
            ]
            atomic_partition_overwrite("mart_production", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_production", dt, run_id)
            return True

        @task
        def build_well_kpi_task(dates, prod_ready):
            contract = MART_CONTRACTS["mart_well_kpi"]
            if not _check_dq_status("production", dates):
                raise AirflowSkipException(
                    f"Upstream DQ != SUCCESS for well_kpi on {dates}"
                )

            sources = {
                name: load_silver_dataset(name, dates, pk_columns=pks)
                for name, pks in contract.source_datasets.items()
            }
            lf_history = load_gold_dataset(TABLE_MART_PRODUCTION)

            lf_result = build_mart_well_kpi(sources["production"], lf_history)
            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_well_kpi")

            staging_table = write_staging_mart(df, "mart_well_kpi")
            actual_dates = [
                str(d)
                for d in df.select("partition_date").unique().to_series().to_list()
            ]
            atomic_partition_overwrite("mart_well_kpi", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_well_kpi", dt, run_id)

        @task
        def build_failures_task(dates):
            contract = MART_CONTRACTS["mart_failures"]
            if not _check_dq_status("pump_sensors", dates):
                raise AirflowSkipException(f"DQ != SUCCESS for pump_sensors on {dates}")

            sources = {
                name: load_silver_dataset(name, dates, pk_columns=pks)
                for name, pks in contract.source_datasets.items()
            }

            lf_result = build_mart_failures(
                sources["pump_sensors"], sources["pump_failures"], sources["pumps"]
            )
            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_failures")

            staging_table = write_staging_mart(df, "mart_failures")
            actual_dates = [
                str(d)
                for d in df.select("partition_date").unique().to_series().to_list()
            ]
            atomic_partition_overwrite("mart_failures", staging_table, actual_dates)
            cleanup_staging(staging_table)

            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_failures", dt, run_id)

        @task
        def build_logistics_task(dates):
            contract = MART_CONTRACTS["mart_logistics"]
            if not _check_dq_status("deliveries", dates):
                raise AirflowSkipException(f"DQ != SUCCESS for deliveries on {dates}")

            sources = {
                name: load_silver_dataset(name, dates, pk_columns=pks)
                for name, pks in contract.source_datasets.items()
            }

            lf_result = build_mart_logistics(
                sources["deliveries"], sources["drivers"], sources["vehicles"]
            )
            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_logistics")

            staging_table = write_staging_mart(df, "mart_logistics")
            actual_dates = [
                str(d)
                for d in df.select("partition_date").unique().to_series().to_list()
            ]
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
