# dags/bronze_to_silver_pipeline.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import polars as pl
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from bronze_to_silver.business_validator import validate_critical_rules
from core.config import TABLE_CONTRACTS, ValidationRule, Severity
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


def _convert_contract_for_bronze_to_silver(table_config):
    """
    Конвертирует TableConfig из единого config.py в формат, 
    совместимый с утилитами bronze_to_silver.
    """
    # Строим columns schema
    columns = dict(table_config.schema)
    
    # Определяем primary_key
    primary_key = table_config.primary_key or []
    
    # Определяем foreign_keys как dict {column: parent_table.parent_column}
    foreign_keys = {}
    fk_columns = set()  # Для исключения из агрегации
    for fk in table_config.foreign_keys:
        foreign_keys[fk.column] = f"{fk.parent_table}.{fk.parent_column}"
        fk_columns.add(fk.column)
    
    # Определяем time_column (partition_column или streaming ordering_key)
    time_column = None
    if table_config.partition_column:
        time_column = table_config.partition_column
    elif table_config.streaming and table_config.streaming.ordering_key:
        time_column = table_config.streaming.ordering_key
    
    # dedup_key: primary_key + time_column если есть
    dedup_key = list(primary_key) if primary_key else []
    if time_column and time_column not in dedup_key:
        dedup_key.append(time_column)
    
    # validation_rules: конвертируем ValidationRule в формат business_validator
    validation_rules = {"enums": {}, "ranges": {}, "custom": []}
    for rule in table_config.validation_rules:
        if rule.rule_type == "enum":
            col = rule.params.get("column")
            values = rule.params.get("values", [])
            if col:
                validation_rules["enums"][col] = values
        elif rule.rule_type == "range":
            col = rule.params.get("column")
            if col:
                range_params = {}
                if "min" in rule.params:
                    range_params["min"] = rule.params["min"]
                if "max" in rule.params:
                    range_params["max"] = rule.params["max"]
                validation_rules["ranges"][col] = range_params
        elif rule.rule_type == "custom":
            expr = rule.params.get("expression")
            if expr:
                severity = rule.severity.value if isinstance(rule.severity, Severity) else str(rule.severity)
                validation_rules["custom"].append({"rule": expr, "severity": severity})
        elif rule.rule_type == "not_null":
            # not_null правила обрабатываются на уровне схемы
            pass
    
    # outlier_columns: пока пустой список, можно расширить позже
    outlier_columns = []
    
    # missing_rules: базовая заглушка, можно расширить
    missing_rules = {}
    
    # aggregation: если есть streaming spec, можно определить агрегацию
    aggregation = None
    if table_config.streaming:
        # Для телеметрии можно определить дневную агрегацию
        if "telemetry" in table_config.table_name or "sensors" in table_config.table_name:
            key_col = None
            # Определяем ключ агрегации из foreign keys или clustering columns
            for fk in table_config.foreign_keys:
                if "well" in fk.parent_table:
                    key_col = "well_id"
                    break
                elif "pump" in fk.parent_table:
                    key_col = "pump_id"
                    break
            
            if key_col and time_column:
                # Определяем числовые колонки для агрегации
                # Исключаем: primary_key, foreign_keys, time_column
                exclude_cols = set(primary_key) | fk_columns
                if time_column:
                    exclude_cols.add(time_column)
                
                numeric_cols = [
                    col for col, dtype in table_config.schema.items() 
                    if dtype.is_numeric() and col not in exclude_cols
                ]
                metrics = {}
                for col in numeric_cols:
                    metrics[col] = ["mean", "max"]
                
                aggregation = {
                    "key": key_col,
                    "time_column": time_column,
                    "granularity": "1d",
                    "metrics": metrics,
                }
    
    return {
        "columns": columns,
        "primary_key": primary_key,
        "foreign_keys": foreign_keys,
        "time_column": time_column,
        "dedup_key": dedup_key,
        "validation_rules": validation_rules,
        "outlier_columns": outlier_columns,
        "missing_rules": missing_rules,
        "aggregation": aggregation,
    }


# Преобразуем TABLE_CONTRACTS в SCHEMA_CONTRACTS формат для бронзовых таблиц
# Фильтруем только bronze слой
SCHEMA_CONTRACTS = {}
for table_name, table_config in TABLE_CONTRACTS.items():
    if table_config.layer == "bronze":
        SCHEMA_CONTRACTS[table_name] = _convert_contract_for_bronze_to_silver(table_config)

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

        # Сначала валидация схемы (до нормализации), чтобы корректно детектить дрифт и missing cols
        # Затем нормализация для приведения типов к контракту
        validate_dataset_schema(lf, dataset, contract["columns"])
        lf = normalize_dataset(lf, dataset, contract)

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

        # Обработка случая пустого списка для concat и выравнивание схем по порядку колонок
        q_rows = 0
        normalized_lfs = []
        expected_meta_cols = ["_quarantine_validation_name", "_quarantine_reason_code"]
        
        for invalid_lf_item in all_invalid_lfs:
            schema = invalid_lf_item.collect_schema()
            lf_with_meta = invalid_lf_item

            # Добавляем недостающие мета-колонки с дефолтными значениями
            for col_name in expected_meta_cols:
                if col_name not in schema:
                    lf_with_meta = lf_with_meta.with_columns(
                        pl.lit("UNKNOWN").alias(col_name)
                    )

            # Выравниваем порядок колонок: сначала данные, потом мета-колонки в фиксированном порядке
            base_columns = [col for col in schema if col not in expected_meta_cols]
            ordered_columns = base_columns + expected_meta_cols
            
            normalized_lfs.append(lf_with_meta.select(ordered_columns))

        if normalized_lfs:
            final_invalid_lf = pl.concat(normalized_lfs, how="vertical")
            q_rows = write_quarantine_dataset(
                invalid_lf=final_invalid_lf,
                dataset=dataset,
                reason_code="DQ_VIOLATION",
                execution_date=execution_date,
                storage_options=storage_options,
            )
        else:
            logger.info(f"No invalid records to quarantine for {dataset}.")

        # Collect strictly before aggregation to prevent double execution and compute accurate watermark
        valid_df = valid_lf.collect()
        
        time_col = contract.get("time_column")
        
        if time_col:
            new_watermark = valid_df[time_col].max()
        else:
            new_watermark = None
            
        processed_rows = len(valid_df)

        if not new_watermark:
            new_watermark = watermark or datetime.strptime(execution_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Convert back to LazyFrame for downstream pipeline operations
        valid_lf = valid_df.lazy()

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

        # Watermark and processed_rows already computed above before aggregation/joins
        # No need to re-collect here

        if not new_watermark:
            new_watermark = watermark or datetime.strptime(execution_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

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
