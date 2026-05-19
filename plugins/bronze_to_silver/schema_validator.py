# plugins/bronze_to_silver/schema_validator.py
import logging
from typing import Dict

import polars as pl

try:
    from airflow.exceptions import AirflowFailException
except ImportError:
    # Fallback for testing without airflow installed
    class AirflowFailException(Exception):
        pass

logger = logging.getLogger(__name__)


def validate_dataset_schema(
    lf: pl.LazyFrame, dataset: str, expected_schema: Dict[str, pl.DataType]
) -> None:
    """
    Проверяет схему данных после нормализации.
    Валидатор проверяет наличие всех обязательных колонок и соответствие типов.
    После нормализации типы должны совпадать с контрактом.
    """
    actual_schema = lf.collect_schema()

    missing_columns = []
    type_mismatches = []

    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            missing_columns.append(col_name)
        else:
            actual_type = actual_schema[col_name]
            # Polars types can be nested or have parameters (like Datetime("ms", "UTC"))
            # We do a base type check for robustness
            if actual_type.base_type() != expected_type.base_type():
                type_mismatches.append(
                    f"{col_name}: expected {expected_type}, got {actual_type}"
                )

    if missing_columns or type_mismatches:
        error_msg = f"Schema validation failed for dataset '{dataset}'.\n"
        if missing_columns:
            error_msg += f"Missing mandatory columns: {missing_columns}\n"
        if type_mismatches:
            error_msg += f"Type mismatches: {type_mismatches}\n"

        logger.error(error_msg)
        raise AirflowFailException(error_msg)

    # Detect new columns (allowed schema drift)
    new_columns = [col for col in actual_schema if col not in expected_schema]
    if new_columns:
        logger.warning(
            f"Dataset '{dataset}' has new unexpected columns: {new_columns}. They will be ignored or passed through."
        )
