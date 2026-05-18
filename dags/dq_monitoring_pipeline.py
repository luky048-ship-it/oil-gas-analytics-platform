from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from core.s3_connection import get_s3_filesystem
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

                if not path.endswith(".parquet") and "*" not in path:
                    path_with_mask = f"{path.rstrip('/')}/*.parquet"
                else:
                    path_with_mask = path

                contract_obj = TABLE_CONTRACTS[ds_name]
                parent_joins = [
                    {
                        "child_key": fk.column,
                        "parent_key": fk.parent_column,
                        "parent_path": f"s3://datalake/silver/{fk.parent_table}/partition_date=*",
                    }
                    for fk in contract_obj.foreign_keys
                ]

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

                if inv_df.height > 0:
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
                logger.info(f"Adding dependency: {parent_name} -> {dataset_name}")
                task_groups[parent_name] >> task_groups[dataset_name]
            else:
                logger.warning(
                    f"Parent table '{parent_name}' for '{dataset_name}' not found in DAG. "
                    "Skipping dependency."
                )


dq_pipeline()
