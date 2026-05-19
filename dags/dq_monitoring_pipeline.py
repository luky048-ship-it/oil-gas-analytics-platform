from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from core.config import TABLE_CONTRACTS as CORE_TABLE_CONTRACTS
from core.s3_connection import get_s3_filesystem
from dq_utils.core import execute_dq_pipeline
from dq_utils.dq_reporter import persist_dq_results
from dq_utils.freshness_validator import validate_data_freshness
from dq_utils.pipeline_status import publish_pipeline_status
from dq_utils.quarantine_writer import write_quarantine_dataset
from dq_utils.s3_utils import (discover_available_partitions,
                               validate_file_integrity)

logger = logging.getLogger(__name__)


def _convert_core_contract_to_dq_format(core_contract):
    """
    Адаптер: конвертирует контракт из plugins/core/config.py (TableConfig)
    в формат, ожидаемый плагинами dq_utils (TableContract).
    """
    from dq_utils.config import ForeignKeyContract, TableContract

    # Конвертация foreign keys
    foreign_keys = [
        ForeignKeyContract(
            column=fk.column,
            parent_table=fk.parent_table,
            parent_column=fk.parent_column,
        )
        for fk in core_contract.foreign_keys
    ]

    # Конвертация value_ranges из validation_rules
    value_ranges = {}
    enums = {}
    custom_rules = []

    for rule in core_contract.validation_rules:
        if rule.rule_type == "range":
            col = rule.params.get("column")
            min_val = rule.params.get("min")
            max_val = rule.params.get("max")
            value_ranges[col] = (min_val, max_val)
        elif rule.rule_type == "enum":
            col = rule.params.get("column")
            values = rule.params.get("values", [])
            enums[col] = values
        elif rule.rule_type == "custom":
            expr = rule.params.get("expression", "")
            # Преобразуем выражение из формата core в формат dq_utils
            # Например: "start_date <= CURRENT_DATE" -> "start_date <= current_date"
            custom_rules.append(expr.replace("CURRENT_DATE", "current_date"))

    # Конвертация freshness_sla_hours в freshness_sla_minutes
    freshness_sla_minutes = None
    if core_contract.freshness_sla_hours is not None:
        freshness_sla_minutes = int(core_contract.freshness_sla_hours * 60)

    return TableContract(
        schema=core_contract.schema,
        primary_keys=core_contract.primary_key,
        not_null_columns=core_contract.not_null_columns,
        foreign_keys=foreign_keys,
        unique_columns=core_contract.unique_columns,
        value_ranges=value_ranges,
        enums=enums,
        custom_rules=custom_rules,
        freshness_sla_minutes=freshness_sla_minutes,
        partition_column=core_contract.partition_column,
        statistical_monitored_columns=[],  # Можно расширить при необходимости
    )


# Создаем адаптированный реестр контрактов для dq_utils
TABLE_CONTRACTS = {
    name: _convert_core_contract_to_dq_format(contract)
    for name, contract in CORE_TABLE_CONTRACTS.items()
}


@dag(
    dag_id="dq_monitoring_pipeline",
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
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")
    task_groups = {}

    for dataset_name, contract in TABLE_CONTRACTS.items():
        with TaskGroup(group_id=f"dq_{dataset_name}") as tg:

            @task
            def discover(ds_name, ds_date):
                hook = PostgresHook(postgres_conn_id="postgres_default")
                sql = """
                    SELECT status 
                    FROM etl_metadata.pipeline_executions 
                    WHERE dataset = %s AND partition_date = %s AND status = 'SUCCESS'
                """
                result = hook.get_first(sql, (ds_name, ds_date))
                if result:
                    return [f"s3://datalake/silver/{ds_name}/partition_date={ds_date}"]
                return []

            @task
            def process_partition(ds_name, path, ds_date):
                logger.info(f"Processing partition: {path}")

                path_with_mask = (
                    f"{path.rstrip('/')}/*.parquet" if "*" not in path else path
                )

                contract_obj = TABLE_CONTRACTS[ds_name]

                parent_joins = []
                for fk in contract_obj.foreign_keys:
                    is_parent_fact = getattr(
                        TABLE_CONTRACTS.get(fk.parent_table), "is_fact", False
                    )

                    if is_parent_fact:
                        p_path = f"s3://datalake/silver/{fk.parent_table}/partition_date={ds_date}/*.parquet"
                    else:
                        p_path = f"s3://datalake/silver/{fk.parent_table}/**/*.parquet"

                    parent_joins.append(
                        {
                            "child_key": fk.column,
                            "parent_key": fk.parent_column,
                            "parent_path": p_path,
                        }
                    )

                fs = get_s3_filesystem()

                file_dq = validate_file_integrity(
                    ds_name, path_with_mask, s3_options={"fs": fs}
                )

                fresh_dq = validate_data_freshness(
                    ds_name,
                    ds_date,
                    contract_obj.freshness_sla_minutes or 1440,
                    s3_options={"fs": fs},
                    base_path="s3://datalake/silver",
                )
                persist_dq_results([fresh_dq.__dict__, file_dq.__dict__], ds_date)

                results, v_df, inv_df = execute_dq_pipeline(
                    dataset=ds_name,
                    partition_path=path_with_mask,
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
                )

                persist_dq_results(results, ds_date)

                if inv_df is not None and len(inv_df) > 0:
                    write_quarantine_dataset(
                        inv_df, ds_name, "core_dq", ds_date, s3_options={"fs": fs}
                    )

                return "SUCCESS"

            @task
            def final_status(ds_name, ds_date):
                publish_pipeline_status(ds_name, ds_date, "SUCCESS")

            paths = discover(dataset_name, "{{ ds }}")
            processed = process_partition.partial(
                ds_name=dataset_name, ds_date="{{ ds }}"
            ).expand(path=paths)
            processed >> final_status(dataset_name, "{{ ds }}")

        task_groups[dataset_name] = tg

    for dataset_name, contract in TABLE_CONTRACTS.items():
        start >> task_groups[dataset_name] >> finish
        for fk in contract.foreign_keys:
            parent_name = fk.parent_table
            if parent_name in task_groups:
                task_groups[parent_name] >> task_groups[dataset_name]


dq_pipeline()
