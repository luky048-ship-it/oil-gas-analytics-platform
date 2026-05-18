from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from core.s3_connection import get_polars_storage_options, get_s3_filesystem
from dq_utils.config import TABLE_CONTRACTS
from dq_utils.core import execute_dq_pipeline
from dq_utils.dq_reporter import persist_dq_results
from dq_utils.freshness_validator import validate_data_freshness
from dq_utils.pipeline_status import publish_pipeline_status
from dq_utils.quarantine_writer import write_quarantine_dataset
from dq_utils.s3_utils import (discover_available_partitions,
                               validate_file_integrity)

logger = logging.getLogger(__name__)


@dag(
    dag_id="dq_monitoring_pipeline_5",
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["production", "dq"],
)
def dq_pipeline():
    """
    Пайплайн мониторинга качества данных (Data Quality).
    Осуществляет проверку целостности файлов, актуальности (freshness),
    соблюдения бизнес-правил и ссылочной целостности для таблиц Silver слоя.
    """
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")

    task_groups = {}

    # Динамическое создание групп задач для каждого набора данных из контракта
    for dataset_name, contract in TABLE_CONTRACTS.items():
        with TaskGroup(group_id=f"dq_{dataset_name}") as tg:

            @task
            def discover(ds_name, ds_date):
                """
                Поиск доступных разделов (partitions) в S3 для указанной даты.
                """
                fs = get_s3_filesystem()
                return discover_available_partitions(
                    ds_name,
                    ds_date,
                    s3_options={"fs": fs},
                    base_path="s3://datalake/silver",
                )

            @task
            def process_partition(ds_name, path, ds_date):
                """
                Выполнение комплекса проверок качества данных для конкретного раздела.
                Включает проверку целостности файлов, свежести данных и выполнение бизнес-правил.
                """
                logger.info(f"Processing partition: {path}")

                polars_opts = get_polars_storage_options()
                fs = get_s3_filesystem()
                contract_obj = TABLE_CONTRACTS[ds_name]

                # Подготовка параметров для проверки ссылочной целостности
                parent_joins = [
                    {
                        "child_key": fk.column,
                        "parent_key": fk.parent_column,
                        "parent_path": f"s3://datalake/silver/{fk.parent_table}/partition_date=*",
                    }
                    for fk in contract_obj.foreign_keys
                ]

                # Базовые проверки: целостность файлов и свежесть данных
                file_dq = validate_file_integrity(ds_name, path, s3_options={"fs": fs})
                fresh_dq = validate_data_freshness(
                    ds_name,
                    ds_date,
                    contract_obj.freshness_sla_minutes or 1440,
                    s3_options={"fs": fs},
                    base_path="s3://datalake/silver",
                )

                persist_dq_results([fresh_dq.__dict__, file_dq.__dict__], ds_date)

                # Запуск основного конвейера DQ (бизнес-правила, статистика, RI)
                results, v_df, inv_df = execute_dq_pipeline(
                    dataset=ds_name,
                    partition_path=path,
                    expected_schema=contract_obj.schema,
                    key_columns=contract_obj.primary_keys,
                    parent_joins=parent_joins,
                    business_rules_config={
                        "not_null_columns": contract_obj.not_null_columns,
                        "value_ranges": contract_obj.value_ranges,
                        "enums": contract_obj.enums,
                        "custom_rules": contract_obj.custom_rules,
                        "statistical_monitored_columns": contract_obj.statistical_monitored_columns,
                    },
                    historical_stats={},
                    execution_date=ds_date,
                    s3_options={"polars": polars_opts, "fs": fs},
                )

                persist_dq_results(results, ds_date)

                # Запись некорректных записей в карантин
                if inv_df.height > 0:
                    write_quarantine_dataset(
                        inv_df, ds_name, "core_dq", ds_date, s3_options={"fs": fs}
                    )

                return "SUCCESS"

            @task
            def final_status(ds_name, ds_date):
                """Публикация итогового статуса проверки качества данных."""
                publish_pipeline_status(ds_name, ds_date, "SUCCESS")

            # Определение зависимостей между задачами внутри группы
            paths = discover(dataset_name, "{{ ds }}")

            processed = process_partition.partial(
                ds_name=dataset_name, ds_date="{{ ds }}"
            ).expand(path=paths)

            processed >> final_status(dataset_name, "{{ ds }}")

        task_groups[dataset_name] = tg

    # Установка зависимостей между таблицами на основе внешних ключей (foreign keys)
    for dataset_name, contract in TABLE_CONTRACTS.items():
        start >> task_groups[dataset_name] >> finish

        for fk in contract.foreign_keys:
            parent_name = fk.parent_table
            if parent_name in task_groups:
                logger.info(f"Adding dependency: {parent_name} -> {dataset_name}")
                task_groups[parent_name] >> task_groups[dataset_name]
            else:
                logger.warning(
                    f"Parent table '{parent_name}' for '{dataset_name}' not found in DAG. "
                    "Skipping dependency."
                )


dq_pipeline()
