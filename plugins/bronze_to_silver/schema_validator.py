import logging
from typing import Dict

import polars as pl
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


def validate_dataset_schema(
    lf: pl.LazyFrame, dataset: str, expected_schema: Dict[str, pl.DataType]
) -> None:
    """
    Проверяет соответствие схемы загруженного набора данных ожидаемой схеме из контракта.
    В случае обнаружения отсутствующих обязательных столбцов или несовпадения типов выбрасывает исключение.
    """
    actual_schema = lf.collect_schema()

    missing_columns = []
    type_mismatches = []

    # Проверка каждого столбца на наличие и соответствие типа
    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            missing_columns.append(col_name)
        else:
            actual_type = actual_schema[col_name]
            # Сравнение базовых типов для обеспечения гибкости (например, для разных единиц времени в Datetime)
            if actual_type.base_type() != expected_type.base_type():
                type_mismatches.append(
                    f"{col_name}: expected {expected_type}, got {actual_type}"
                )

    # Формирование и выброс ошибки при критических несоответствиях
    if missing_columns or type_mismatches:
        error_msg = f"Schema validation failed for dataset '{dataset}'.\n"
        if missing_columns:
            error_msg += f"Missing mandatory columns: {missing_columns}\n"
        if type_mismatches:
            error_msg += f"Type mismatches: {type_mismatches}\n"

        logger.error(error_msg)
        raise AirflowFailException(error_msg)

    # Регистрация предупреждения о появлении новых (неописанных) столбцов
    new_columns = [col for col in actual_schema if col not in expected_schema]
    if new_columns:
        logger.warning(
            f"Dataset '{dataset}' has new unexpected columns: {new_columns}. They will be ignored or passed through."
        )
