from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict

import polars as pl
from airflow.exceptions import AirflowFailException

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_schema_contract(
    lf: pl.LazyFrame, expected_schema: Dict[str, pl.DataType], dataset: str
) -> DQResult:
    """
    Выполняет проверку физической схемы набора данных на соответствие контракту.
    Проверяет наличие обязательных столбцов и соответствие типов данных без материализации данных.
    Выбрасывает AirflowFailException при критических расхождениях схемы.
    """
    actual_schema = lf.schema

    missing_columns = []
    type_mismatches = []
    unexpected_columns = []

    # Проверка каждого ожидаемого столбца
    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            missing_columns.append(col_name)
        else:
            actual_type = actual_schema[col_name]
            # Строгая проверка типов данных согласно корпоративному контракту
            if actual_type != expected_type:
                type_mismatches.append(
                    f"{col_name} (expected {expected_type}, got {actual_type})"
                )

    # Идентификация не задекларированных столбцов
    for col_name in actual_schema.keys():
        if col_name not in expected_schema:
            unexpected_columns.append(col_name)

    # Формирование ошибки при нарушении целостности схемы
    if missing_columns or type_mismatches:
        error_msg = (
            f"Schema contract violation for '{dataset}'. "
            f"Missing: {missing_columns}. Mismatches: {type_mismatches}."
        )
        logger.error(error_msg)
        raise AirflowFailException(f"CRITICAL: {error_msg}")

    # Предупреждение о наличии новых столбцов (schema evolution)
    if unexpected_columns:
        logger.warning(
            f"Unexpected columns found in '{dataset}' (allowed by evolution policy): {unexpected_columns}"
        )

    return DQResult(
        dataset=dataset,
        validation_type="Schema Contract Validation",
        status="PASS",
        failed_rows=0,
        checked_rows=0,
        message="Schema matches contract successfully.",
        created_at=datetime.utcnow(),
    )
