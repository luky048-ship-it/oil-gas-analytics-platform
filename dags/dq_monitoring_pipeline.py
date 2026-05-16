from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from dq_utils.config import TABLE_CONTRACTS
from dq_utils.core import execute_dq_pipeline
from dq_utils.dq_reporter import persist_dq_results
from dq_utils.freshness_validator import validate_data_freshness
from dq_utils.pipeline_status import publish_pipeline_status
from dq_utils.quarantine_writer import write_quarantine_dataset
from dq_utils.s3_utils import (discover_available_partitions,
                               get_s3_storage_options, validate_file_integrity)

logger = logging.getLogger(__name__)


@dag(
    dag_id="dq_monitoring_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    max_active_runs=1,
    catchup=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["production", "dq"],
)
def dq_pipeline():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")

    @task
    def get_s3_opts():
        return get_s3_storage_options("aws_default")

    s3_opts = get_s3_opts()

    for dataset_name, contract in TABLE_CONTRACTS.items():
        with TaskGroup(group_id=f"dq_{dataset_name}") as tg:

            @task
            def discover(ds_name, opts, **kwargs):
                return discover_available_partitions(ds_name, kwargs["ds"], opts)

            @task
            def process(ds_name, paths, opts, **kwargs):
                exec_date = kwargs["ds"]
                contract_obj = TABLE_CONTRACTS[ds_name]

                # RI Configuration
                parent_joins = []
                for fk in contract_obj.foreign_keys:
                    parent_joins.append(
                        {
                            "child_key": fk.column,
                            "parent_key": fk.parent_column,
                            "parent_path": f"s3://datalake/silver/{fk.parent_table}/partition_date=*",
                        }
                    )

                all_dq_results = []

                for p in paths:
                    # Валидация файла (Layer 1)
                    file_dq = validate_file_integrity(ds_name, p, opts)
                    # Валидация свежести (Layer 5)
                    fresh_dq = validate_data_freshness(ds_name, exec_date, contract_obj.freshness_sla_minutes or 1440, opts)
                    all_dq_results.append(fresh_dq.__dict__)
                    all_dq_results[-1]["created_at"] = all_dq_results[-1]["created_at"].isoformat()
                    all_dq_results.append(file_dq.__dict__)
                    # Сериализация даты для XCom
                    all_dq_results[-1]["created_at"] = all_dq_results[-1][
                        "created_at"
                    ].isoformat()

                    # Основной процессинг (Layer 2-4, 7)
                    results, v_df, inv_df = execute_dq_pipeline(
                        dataset=ds_name,
                        partition_path=p,
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
                        execution_date=exec_date,
                        s3_options=opts,
                    )

                    all_dq_results.extend(results)

                    # Запись Silver (Используем pyarrow_options для S3)
                    if v_df.height > 0:
                        target = f"s3://datalake/silver/{ds_name}/partition_date={exec_date}/data.parquet"
                        v_df.write_parquet(
                            target,
                            use_pyarrow=True,
                            pyarrow_options={"storage_options": opts},
                        )

                    # Запись Quarantine
                    if inv_df.height > 0:
                        write_quarantine_dataset(
                            inv_df, ds_name, "core_dq", exec_date, opts
                        )

                return all_dq_results

            @task
            def report(ds_name, dq_res, **kwargs):
                persist_dq_results(dq_res, kwargs["ds"])

            @task(trigger_rule="all_done")
            def status(ds_name, **kwargs):
                ti = kwargs["ti"]
                # Проверка статуса таски process в этой группе
                state = (
                    "SUCCESS"
                    if ti.xcom_pull(task_ids=f"{tg.group_id}.process")
                    else "FAILED"
                )
                publish_pipeline_status(ds_name, kwargs["ds"], state)

            # Flow
            p_list = discover(dataset_name, s3_opts)
            dq_data = process(dataset_name, p_list, s3_opts)
            report(dataset_name, dq_data) >> status(dataset_name)

        start >> s3_opts >> tg >> finish


dq_pipeline()
