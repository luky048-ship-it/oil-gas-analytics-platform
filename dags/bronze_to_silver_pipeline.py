# dags/bronze_to_silver_pipeline.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from bronze_to_silver.business_validator import validate_critical_rules
from bronze_to_silver.config import SCHEMA_CONTRACTS
from bronze_to_silver.deduplicator import deduplicate_dataset
from bronze_to_silver.enricher import enrich_reference_data
from bronze_to_silver.event_time_aggregator import aggregate_event_time_metrics
from bronze_to_silver.metadata_utils import (get_last_watermark,
                                             publish_pipeline_metadata,
                                             update_pipeline_watermark)
from bronze_to_silver.missing_handler import handle_missing_values
from bronze_to_silver.normalizer import normalize_dataset
from bronze_to_silver.outlier_detector import detect_outliers
from bronze_to_silver.partition_discovery import \
    discover_incremental_partitions
from bronze_to_silver.pipeline_execution import PipelineExecutionResult
from bronze_to_silver.quarantine_writer import write_quarantine_dataset
from bronze_to_silver.s3_utils import (get_s3_storage_options,
                                       load_bronze_dataset)
from bronze_to_silver.schema_validator import validate_dataset_schema
from bronze_to_silver.silver_writer import write_silver_dataset

logger = logging.getLogger(__name__)

METADATA_CONN_ID = "postgres_default"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
    "sla": timedelta(hours=2),
    "pool": "silver_processing",
}

with DAG(
    dag_id="bronze_to_silver_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "silver", "medallion", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")

    @task(multiple_outputs=False)
    def discover_partitions(dataset: str, **context) -> list[str]:
        storage_options = get_s3_storage_options()
        watermark = get_last_watermark(dataset, conn_id=METADATA_CONN_ID)

        logger.info(f"Discovering partitions for {dataset} with watermark: {watermark}")
        partitions = discover_incremental_partitions(
            dataset=dataset, watermark=watermark, storage_options=storage_options
        )
        logger.info(f"Found {len(partitions)} new partitions for {dataset}.")
        return partitions

    @task(multiple_outputs=False)
    def process_dataset(dataset: str, partition_paths: list[str], **context) -> dict:
        if not partition_paths:
            logger.info(f"No new partitions to process for {dataset}.")
            return {"status": "skipped", "dataset": dataset}

        t_start = datetime.now()
        execution_date = context["ds"]
        storage_options = get_s3_storage_options()
        contract = SCHEMA_CONTRACTS[dataset]
        watermark = get_last_watermark(dataset, conn_id=METADATA_CONN_ID)

        lf = load_bronze_dataset(
            dataset_paths=partition_paths,
            storage_options=storage_options,
            watermark=watermark,
            time_column=contract.get("time_column"),
        )

        # Сначала нормализация (приведение типов), затем валидация схемы
        # Это позволяет легально кастить Decimal -> Float64 и другие совместимые типы
        lf = normalize_dataset(lf, dataset, contract)
        validate_dataset_schema(lf, dataset, contract["columns"])

        valid_lf, business_invalid_lf = validate_critical_rules(
            lf, contract.get("validation_rules", {})
        )

        valid_lf = deduplicate_dataset(
            valid_lf,
            key_columns=contract.get("dedup_key"),
            timestamp_column=contract.get("time_column"),
        )

        valid_lf = handle_missing_values(
            valid_lf, dataset, contract.get("missing_rules", {})
        )

        valid_lf, outlier_invalid_lf = detect_outliers(
            valid_lf,
            dataset,
            monitored_columns=contract.get("outlier_columns", []),
            method="iqr",
            multiplier=3.0,
        )

        all_invalid_lfs = []
        if business_invalid_lf is not None:
            all_invalid_lfs.append(business_invalid_lf)
        if outlier_invalid_lf is not None:
            all_invalid_lfs.append(outlier_invalid_lf)

        q_rows = 0
        normalized_lfs = []
        for invalid_lf_item in all_invalid_lfs:
            schema = invalid_lf_item.collect_schema()
            lf_with_meta = invalid_lf_item

            if "_quarantine_validation_name" not in schema:
                lf_with_meta = lf_with_meta.with_columns(
                    pl.lit("UNKNOWN_VALIDATION").alias("_quarantine_validation_name")
                )
            if "_quarantine_reason_code" not in schema:
                lf_with_meta = lf_with_meta.with_columns(
                    pl.lit("UNKNOWN_REASON").alias("_quarantine_reason_code")
                )

            normalized_lfs.append(lf_with_meta)

        # Обработка случая пустого списка для concat
        if normalized_lfs:
            final_invalid_lf = pl.concat(normalized_lfs)
            q_rows = write_quarantine_dataset(
                invalid_lf=final_invalid_lf,
                dataset=dataset,
                reason_code="DQ_VIOLATION",
                execution_date=execution_date,
                storage_options=storage_options,
            )
        else:
            logger.info(f"No invalid records to quarantine for {dataset}.")

        if "aggregation" in contract:
            valid_lf = aggregate_event_time_metrics(
                valid_lf, dataset, contract["aggregation"]
            )

        if "joins" in contract:
            for join_def in contract["joins"]:
                valid_lf = enrich_reference_data(
                    lf=valid_lf,
                    reference_dataset=f"s3://datalake/silver/{join_def['ref_dataset']}",
                    join_key=join_def["key"],
                    storage_options=storage_options,
                    how=join_def.get("how", "left"),
                )

        output_path = write_silver_dataset(
            lf=valid_lf,
            dataset=dataset,
            partition_date=execution_date,
            storage_options=storage_options,
            time_column=contract.get("time_column"),
        )

        time_col = contract.get("time_column")

        if time_col and "aggregation" not in contract:
            metrics_df = valid_lf.select(
                [pl.len().alias("count"), pl.col(time_col).max().alias("max_time")]
            ).collect()

            processed_rows = metrics_df["count"].item(0)
            new_watermark = metrics_df["max_time"].item(0)
        else:
            processed_rows = valid_lf.select(pl.len()).collect().item()
            new_watermark = None

        if not new_watermark:
            new_watermark = watermark or datetime.strptime(execution_date, "%Y-%m-%d")

        t_end = datetime.now()
        execution_time = (t_end - t_start).total_seconds()

        result = PipelineExecutionResult(
            dataset=dataset,
            partition_date=execution_date,
            processed_rows=processed_rows,
            quarantined_rows=q_rows,
            output_path=output_path,
            execution_time_sec=execution_time,
            watermark=new_watermark,
        )

        update_pipeline_watermark(
            dataset, new_watermark, execution_date, conn_id=METADATA_CONN_ID
        )
        publish_pipeline_metadata(result, conn_id=METADATA_CONN_ID)

        logger.info(
            f"Successfully processed {dataset}: {processed_rows} rows. Watermark advanced to {new_watermark}."
        )
        return result.__dict__

    for ds_name in SCHEMA_CONTRACTS.keys():
        with TaskGroup(group_id=f"process_group_{ds_name}") as tg:

            discovered_paths = discover_partitions.override(
                task_id=f"discover_{ds_name}"
            )(dataset=ds_name)

            processed_result = process_dataset.override(task_id=f"process_{ds_name}")(
                dataset=ds_name, partition_paths=discovered_paths
            )

            discovered_paths >> processed_result

        start >> tg >> finish
