import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.config import TABLE_CONTRACTS as CORE_TABLE_CONTRACTS
from gold_layer.builders.mart_failures import build_mart_failures
from gold_layer.builders.mart_logistics import build_mart_logistics
from gold_layer.builders.mart_production import build_mart_production
from gold_layer.builders.mart_well_kpi import build_mart_well_kpi
from gold_layer.constants import SILVER_PRODUCTION, TABLE_MART_PRODUCTION
from gold_layer.loaders import (discover_new_partitions, load_gold_dataset,
                                load_silver_dataset)
from gold_layer.publishers import (atomic_partition_overwrite, cleanup_staging,
                                   write_staging_mart)
from gold_layer.validators import (validate_business_readiness,
                                   validate_mart_before_publish)
from gold_layer.watermarks import get_last_watermark, update_mart_watermark

# Импортируем локальный MartConfig для создания адаптированных контрактов
from gold_layer.config import MartConfig

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "pool": "gold_pool",
}


def _convert_core_contract_to_mart_format(core_contracts):
    """
    Адаптер: конвертирует контракты из plugins/core/config.py (TableConfig)
    в формат MartConfig, ожидаемый плагинами gold_layer.
    
    Золотой слой работает с mart-таблицами, которые агрегируют данные из silver.
    Мы создаем конфигурацию mart-таблиц на основе информации о source_datasets
    из оригинального gold_layer/config.py, но используем core.config как источник
    правды для схем и валидаций исходных таблиц.
    """
    # Определяем mapping mart -> source datasets (из старого gold_layer/config.py)
    mart_sources = {
        "mart_production": {
            "production": ["prod_id"],
            "well_telemetry": ["record_id"],
            "well_targets": ["well_id", "date"],
        },
        "mart_well_kpi": {
            "production": ["prod_id"],
        },
        "mart_failures": {
            "pump_sensors": ["record_id"],
            "pump_failures": ["failure_id"],
            "pumps": ["pump_id"],
        },
        "mart_logistics": {
            "deliveries": ["delivery_id"],
            "drivers": ["driver_id"],
            "vehicles": ["vehicle_id"],
        },
    }

    # Бизнес-правила для mart-таблиц (из старого gold_layer/config.py)
    mart_business_rules = {
        "mart_production": {"min_oil_ton": 0},
        "mart_well_kpi": {},
        "mart_failures": {"max_z_score": 10.0},
        "mart_logistics": {},
    }

    # Критические колонки и уникальные ключи для mart-таблиц
    mart_critical_columns = {
        "mart_production": ["well_id", "date"],
        "mart_well_kpi": ["well_id", "date"],
        "mart_failures": ["pump_id", "date", "timestamp"],
        "mart_logistics": ["delivery_id", "date"],
    }

    mart_unique_keys = {
        "mart_production": ["well_id", "date"],
        "mart_well_kpi": ["well_id", "date"],
        "mart_failures": ["pump_id", "timestamp"],
        "mart_logistics": ["delivery_id"],
    }

    # Создаем MartConfig для каждой mart-таблицы
    mart_contracts = {}
    for mart_name, sources in mart_sources.items():
        mart_contracts[mart_name] = MartConfig(
            table_name=f"gold.{mart_name}",
            critical_columns=mart_critical_columns[mart_name],
            unique_key=mart_unique_keys[mart_name],
            business_rules=mart_business_rules[mart_name],
            source_datasets=sources,
        )

    return mart_contracts


# Создаем адаптированный реестр mart-контрактов для gold_layer
MART_CONTRACTS = _convert_core_contract_to_mart_format(CORE_TABLE_CONTRACTS)


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
