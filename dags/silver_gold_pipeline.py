# dags/silver_gold_pipeline.py
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from gold_layer.config import ANALYSIS_PARAMS, MART_CONTRACTS
from gold_layer.constants import (SILVER_DELIVERIES, SILVER_DRIVERS,
                                  SILVER_PRODUCTION, SILVER_PUMP_FAILURES,
                                  SILVER_PUMP_SENSORS, SILVER_TARGETS,
                                  SILVER_TELEMETRY, SILVER_VEHICLES,
                                  TABLE_MART_PRODUCTION)
from gold_layer.generic_builder import build_mart
from gold_layer.loaders import (discover_new_partitions, load_gold_dataset,
                                load_silver_dataset)
from gold_layer.watermarks import get_last_watermark, update_mart_watermark
from gold_layer.writers import write_mart

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "pool": "gold_pool",
}

with DAG(
    dag_id="silver_gold_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["gold", "production", "petroleum"],
    render_template_as_native_obj=True,
) as dag:

    finish = EmptyOperator(task_id="finish")

    @task
    def parse_execution_dates(**context) -> list[str]:
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        start_str = conf.get("start_date")
        end_str = conf.get("end_date")

        if start_str and end_str:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            dates = []
            cur = start_date
            while cur <= end_date:
                dates.append(cur.isoformat())
                cur += timedelta(days=1)
            logger.info(f"Execution dates resolved from upstream conf: {dates}")
            return dates

        logger.warning(
            "No date parameters provided in conf. Falling back to watermark discovery."
        )
        last_dt = get_last_watermark(TABLE_MART_PRODUCTION)
        new_dates = discover_new_partitions(SILVER_PRODUCTION, last_dt)
        if not new_dates:
            logger.info("No new partitions discovered.")
            return []

        logger.info(f"Execution dates resolved via watermark: {new_dates}")
        return sorted(new_dates)

    dates = parse_execution_dates()

    with TaskGroup(group_id="process_marts") as process_marts:

        @task
        def build_production_task(dates: list[str]) -> None:
            if not dates:
                return

            lf_prod = load_silver_dataset(SILVER_PRODUCTION, dates)
            if lf_prod is None:
                logger.warning(
                    "No production data found for dates: %s. Skipping mart generation.",
                    dates,
                )
                return

            context = get_current_context()
            run_id = context["run_id"]
            spec = MART_CONTRACTS["mart_production"]

            lf_tele = load_silver_dataset(SILVER_TELEMETRY, dates)
            lf_targ = load_silver_dataset(SILVER_TARGETS, dates)

            lf_result = build_mart(
                spec,
                {
                    "production": lf_prod,
                    "well_telemetry": lf_tele,
                    "well_targets": lf_targ,
                },
            )

            df = lf_result.collect()
            write_mart(df, spec, dates)

            for dt in dates:
                update_mart_watermark(spec.table_name, dt, run_id)

        @task
        def build_well_kpi_task(dates: list[str]) -> None:
            if not dates:
                return

            context = get_current_context()
            run_id = context["run_id"]

            min_date_str = min(dates)
            max_date_str = max(dates)
            min_date_dt = datetime.strptime(min_date_str, "%Y-%m-%d")

            batch_query = f"""
                SELECT * FROM {TABLE_MART_PRODUCTION} 
                WHERE date >= '{min_date_str}' AND date <= '{max_date_str}'
            """
            lf_prod_batch = load_gold_dataset(TABLE_MART_PRODUCTION, query=batch_query)

            if lf_prod_batch.collect_schema().len() == 0:
                logger.warning(
                    "Target table gold.mart_production is empty. Skipping KPI mart."
                )
                return

            window_days = ANALYSIS_PARAMS.get("kpi_rolling_window", 7)
            hist_min_date = (min_date_dt - timedelta(days=window_days)).strftime(
                "%Y-%m-%d"
            )

            history_query = f"""
                SELECT * FROM {TABLE_MART_PRODUCTION} 
                WHERE date >= '{hist_min_date}' AND date < '{min_date_str}'
            """
            lf_history = load_gold_dataset(TABLE_MART_PRODUCTION, query=history_query)

            spec = MART_CONTRACTS["mart_well_kpi"]
            lf_result = build_mart(
                spec,
                {
                    "mart_production_batch": lf_prod_batch,
                    "mart_production_history": lf_history,
                },
            )

            df = lf_result.collect()
            write_mart(df, spec, dates)

            for dt in dates:
                update_mart_watermark(spec.table_name, dt, run_id)

        @task
        def build_failures_task(dates: list[str]) -> None:
            if not dates:
                return

            lf_sensors = load_silver_dataset(SILVER_PUMP_SENSORS, dates)
            if lf_sensors is None:
                logger.warning(
                    "No pump sensors data found for dates: %s. Skipping mart generation.",
                    dates,
                )
                return

            context = get_current_context()
            run_id = context["run_id"]
            spec = MART_CONTRACTS["mart_failures"]

            lf_failures = load_silver_dataset(SILVER_PUMP_FAILURES, dates)
            lf_pumps = load_silver_dataset("pumps")

            lf_result = build_mart(
                spec,
                {
                    "pump_sensors": lf_sensors,
                    "pump_failures": lf_failures,
                    "pumps": lf_pumps,
                },
            )

            df = lf_result.collect()
            write_mart(df, spec, dates)

            for dt in dates:
                update_mart_watermark(spec.table_name, dt, run_id)

        @task
        def build_logistics_task(dates: list[str]) -> None:
            if not dates:
                return

            lf_deliveries = load_silver_dataset(SILVER_DELIVERIES, dates)
            if lf_deliveries is None:
                logger.warning(
                    "No deliveries data found for dates: %s. Skipping mart generation.",
                    dates,
                )
                return

            context = get_current_context()
            run_id = context["run_id"]
            spec = MART_CONTRACTS["mart_logistics"]

            lf_drivers = load_silver_dataset(SILVER_DRIVERS)
            lf_vehicles = load_silver_dataset(SILVER_VEHICLES)

            lf_result = build_mart(
                spec,
                {
                    "deliveries": lf_deliveries,
                    "drivers": lf_drivers,
                    "vehicles": lf_vehicles,
                },
            )

            df = lf_result.collect()
            write_mart(df, spec, dates)

            for dt in dates:
                update_mart_watermark(spec.table_name, dt, run_id)

        prod = build_production_task(dates)
        well_kpi = build_well_kpi_task(dates)
        failures = build_failures_task(dates)
        logistics = build_logistics_task(dates)

        prod >> well_kpi
        [well_kpi, failures, logistics] >> finish
