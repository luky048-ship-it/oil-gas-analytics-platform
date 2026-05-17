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

    @task
    def discover_dates():
        last_dt = get_last_watermark("mart_production")
        new_dates = discover_new_partitions(SILVER_PRODUCTION, last_dt)
        if not new_dates:
            logging.info("No new partitions discovered in Silver.")
        return new_dates

    @task_group(group_id="process_marts")
    def process_marts(partition_dates):

        def _check_dq_status(dataset_name: str, dates: list[str]) -> bool:
            """Проверяет статус DQ в мета-БД. Возвращает True только если SUCCESS."""
            hook = PostgresHook(postgres_conn_id="postgres_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Настройте таблицу и поля под вашу схему мета-данных
                    cur.execute(
                        """
                        SELECT DISTINCT status FROM etl_metadata.dq_validation_results 
                        WHERE dataset = %s AND partition_date = ANY(%s)
                    """,
                        (dataset_name, dates),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        logging.warning(
                            f"No DQ status found for {dataset_name} on {dates}. Treating as UNKNOWN."
                        )
                        return False
                    statuses = {row[0] for row in rows}
                    if "SUCCESS" in statuses and len(statuses) == 1:
                        return True
                    logging.warning(
                        f"DQ status for {dataset_name} is {statuses}, not strictly SUCCESS."
                    )
                    return False

        def _filter_quarantined(
            lf: pl.LazyFrame,
            dataset_name: str,
            dates: list[str],
            unique_keys: list[str],
        ) -> pl.LazyFrame:
            """Загружает ID из карантина и применяет anti-join, оставляя только чистые данные."""
            # Пример загрузки карантина из БД. Если у вас карантин в S3/MinIO, замените на load_s3_quarantine(...)
            hook = PostgresHook(postgres_conn_id="postgres_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT {} FROM etl_metadata.quarantine_keys
                        WHERE dataset = %s AND partition_date = ANY(%s)
                    """.format(", ".join(unique_keys)),
                        (dataset_name, dates),
                    )
                    rows = cur.fetchall()

            if not rows:
                logging.info(
                    f"No quarantine records for {dataset_name} on {dates}. Skipping filter."
                )
                return lf

            quarantine_lf = pl.DataFrame(rows, schema=unique_keys).lazy()
            logging.info(
                f"Filtering out {quarantine_lf.collect().shape[0]} quarantined records for {dataset_name}."
            )
            return lf.join(quarantine_lf, on=unique_keys, how="anti")

        @task
        def build_production_task(dates):
            lf_prod = load_silver_dataset(SILVER_PRODUCTION, dates)
            lf_tele = load_silver_dataset(SILVER_TELEMETRY, dates)
            lf_targ = load_silver_dataset(SILVER_TARGETS, dates)

            lf_result = build_mart_production(lf_prod, lf_tele, lf_targ)
            lf_result = validate_business_readiness(lf_result, "mart_production")

            df = lf_result.collect()
            validate_mart_before_publish(df, "mart_production")

            staging_table = write_staging_mart(df, "mart_production")

            actual_dates = df.select("partition_date").unique().to_series().to_list()
            actual_dates = [str(d) for d in actual_dates]

            atomic_partition_overwrite("mart_production", staging_table, actual_dates)
            cleanup_staging(staging_table)

            # Update watermarks for each processed date
            run_id = "{{ run_id }}"
            for dt in actual_dates:
                update_mart_watermark("mart_production", dt, run_id)

            return True  # Signal for dependent marts

        @task
        def build_well_kpi_task(dates, prod_ready):

            lf_prod_batch = build_mart_production(
                load_silver_dataset(SILVER_PRODUCTION, dates),
                load_silver_dataset(SILVER_TELEMETRY, dates),
                load_silver_dataset(SILVER_TARGETS, dates),
            )

            lf_history = load_gold_dataset(TABLE_MART_PRODUCTION)

            lf_result = build_mart_well_kpi(lf_prod_batch, lf_history)
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
            lf_sens = load_silver_dataset(SILVER_PUMP_SENSORS, dates)
            lf_fail = load_silver_dataset(SILVER_PUMP_FAILURES, dates)
            # Pumps is likely a dimension, load entirely or filter
            lf_pumps = load_silver_dataset("pumps")

            lf_result = build_mart_failures(lf_sens, lf_fail, lf_pumps)
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
            lf_del = load_silver_dataset(SILVER_DELIVERIES, dates)
            lf_drv = load_silver_dataset(SILVER_DRIVERS)
            lf_veh = load_silver_dataset(SILVER_VEHICLES)

            lf_result = build_mart_logistics(lf_del, lf_drv, lf_veh)
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
