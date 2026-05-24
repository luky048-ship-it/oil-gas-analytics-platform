# dags/bronze_to_silver_pipeline.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import polars as pl
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from bronze_to_silver.config import SCHEMA_CONTRACTS
from bronze_to_silver.deduplicator import deduplicate_dataset
from bronze_to_silver.metadata_utils import (get_bronze_partitions_from_db,
                                             publish_pipeline_metadata,
                                             update_pipeline_watermark)
from bronze_to_silver.missing_handler import handle_missing_values
from bronze_to_silver.normalizer import normalize_dataset
from bronze_to_silver.outlier_detector import detect_outliers
from bronze_to_silver.pipeline_execution import PipelineExecutionResult
from bronze_to_silver.quarantine_writer import write_quarantine_dataset
from bronze_to_silver.s3_utils import (get_s3_storage_options,
                                       load_bronze_dataset)
from bronze_to_silver.schema_validator import (filter_by_data_quality,
                                               validate_dataset_schema)
from bronze_to_silver.silver_writer import write_silver_dataset

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "pool": "silver_processing",
}

with DAG(
    dag_id="bronze_to_silver_pipeline",
    schedule=None,
    start_date=datetime(2025, 10, 1),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "silver", "medallion", "production"],
    render_template_as_native_obj=True,
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")

    @task
    def parse_execution_dates(**context) -> List[str]:
        """Извлекает диапазон дат из conf (бэкфилл) или использует ds."""
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}

        start_str = conf.get("start_date", context["ds"])
        end_str = conf.get("end_date", context["ds"])

        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

        dates = []
        cur_date = start_date
        while cur_date <= end_date:
            dates.append(cur_date.isoformat())
            cur_date += timedelta(days=1)

        logger.info(f"Execution dates parsed: {dates}")
        return dates

    @task
    def get_partitions_for_date(
        dataset: str, target_date: str, is_fact: bool, **context
    ) -> Dict[str, Any]:
        """Получает пути к данным из метаданных Bronze для конкретной даты."""
        paths = get_bronze_partitions_from_db(
            table_name=dataset,
            start_date=target_date,
            end_date=target_date,
            is_fact=is_fact,
        )
        return {
            "dataset": dataset,
            "target_date": target_date,
            "is_fact": is_fact,
            "paths": paths,
        }

    @task
    def process_dataset(payload: Dict[str, Any], **context) -> Dict[str, Any]:
        """Основной ETL процесс для одного датасета и одной даты (или full snapshot)."""
        dataset = payload["dataset"]
        target_date = payload["target_date"]
        is_fact = payload["is_fact"]
        partition_paths = payload["paths"]

        if not partition_paths:
            logger.info(
                f"No bronze data found for {dataset} on {target_date}. Skipping."
            )
            return {"status": "skipped", "dataset": dataset, "date": target_date}

        t_start = datetime.now()
        storage_options = get_s3_storage_options()
        contract = SCHEMA_CONTRACTS[dataset]

        # 1. Lazy Load
        lf = load_bronze_dataset(
            dataset_paths=partition_paths,
            storage_options=storage_options,
            watermark=None,
            time_column=contract.get("time_column"),
        )

        # 2. Validate Schema
        schema_valid_lf, schema_invalid_lf = validate_dataset_schema(
            lf=lf,
            dataset=dataset,
            expected_schema=contract["columns"],
        )

        schema_q_rows = 0
        if schema_invalid_lf is not None:
            schema_q_rows = write_quarantine_dataset(
                invalid_lf=schema_invalid_lf,
                dataset=dataset,
                reason_code="schema_violation",
                execution_date=target_date,
                storage_options=storage_options,
            )

        # Если все строки невалидны из-за схемы — пропускаем дальнейшую обработку
        if schema_q_rows > 0 and schema_valid_lf.select(pl.len()).collect().item() == 0:
            logger.warning(
                f"All rows quarantined due to schema violations. Skipping transformations."
            )
            return {
                "status": "skipped",
                "dataset": dataset,
                "date": target_date,
                "quarantined": schema_q_rows,
            }

        lf = schema_valid_lf

        # 3 DQ
        dq_valid_lf, dq_invalid_lf = filter_by_data_quality(
            lf, validation_rules=contract.get("validation_rules", {})
        )

        dq_q_rows = 0
        if dq_invalid_lf is not None:
            dq_q_rows = write_quarantine_dataset(
                invalid_lf=dq_invalid_lf,
                dataset=dataset,
                reason_code="dq_violation",
                execution_date=target_date,
                storage_options=storage_options,
            )

        lf = dq_valid_lf

        # 4-6. Transformations
        lf = normalize_dataset(lf, dataset, contract)
        lf = deduplicate_dataset(
            lf,
            key_columns=contract.get("dedup_key"),
            timestamp_column=contract.get("time_column"),
        )
        lf = handle_missing_values(lf, contract.get("missing_rules", {}))

        valid_lf, invalid_lf = detect_outliers(
            lf,
            monitored_columns=contract.get("outlier_columns", []),
            method="iqr",
            multiplier=3.0,
        )

        # 7. quarantine
        out_q_rows = 0
        if invalid_lf is not None:
            out_q_rows = write_quarantine_dataset(
                invalid_lf=invalid_lf,
                dataset=dataset,
                reason_code="outlier",
                execution_date=target_date,
                storage_options=storage_options,
            )

        # 8. Write Silver
        output_path = write_silver_dataset(
            lf=valid_lf,
            dataset=dataset,
            partition_date=target_date if is_fact else None,
            storage_options=storage_options,
        )

        # 9. Metrics & Watermark
        processed_rows = valid_lf.select(pl.len()).collect().item()
        q_rows = schema_q_rows + dq_q_rows + out_q_rows
        new_watermark = None
        if is_fact and contract.get("time_column"):
            max_time = (
                valid_lf.select(pl.col(contract["time_column"]).max()).collect().item()
            )
            if max_time:
                new_watermark = max_time
                update_pipeline_watermark(dataset, new_watermark, target_date)

        t_end = datetime.now()
        result = PipelineExecutionResult(
            dataset=dataset,
            partition_date=target_date,
            processed_rows=processed_rows,
            quarantined_rows=q_rows,
            output_path=output_path,
            execution_time_sec=(t_end - t_start).total_seconds(),
            watermark=new_watermark,
        )

        publish_pipeline_metadata(result)
        logger.info(f"Processed {dataset} for {target_date}: {processed_rows} rows.")

        return {
            "dataset": result.dataset,
            "partition_date": str(result.partition_date),
            "processed_rows": result.processed_rows,
            "quarantined_rows": result.quarantined_rows,
            "status": "success",
        }

    # --- Оркестрация ---
    dates_list = parse_execution_dates()

    dimension_groups = []
    fact_groups = []

    for ds_name, cfg in SCHEMA_CONTRACTS.items():
        is_fact = cfg.get("is_fact", False)

        with TaskGroup(group_id=f"process_{ds_name}") as tg:
            if not is_fact:
                partition_payload = get_partitions_for_date(
                    dataset=ds_name, target_date="1900-01-01", is_fact=False
                )
                process_dataset(partition_payload)
            else:
                payloads = get_partitions_for_date.expand(
                    dataset=ds_name, target_date=dates_list, is_fact=is_fact
                )
                process_dataset.expand(payload=payloads)

        if is_fact:
            fact_groups.append(tg)
        else:
            dimension_groups.append(tg)

    start >> dimension_groups >> fact_groups >> finish

    trigger_gold_dag = TriggerDagRunOperator(
        task_id="trigger_silver_gold_pipeline",
        trigger_dag_id="silver_gold_pipeline",
        conf="{{ dag_run.conf }}",
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    finish >> trigger_gold_dag
