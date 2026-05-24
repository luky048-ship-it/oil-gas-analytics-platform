# /plugins/bronze_to_silver/schema_validator.py
import logging
from typing import Any, Dict, Tuple

import polars as pl

logger = logging.getLogger(__name__)


def validate_dataset_schema(
    lf: pl.LazyFrame,
    dataset: str,
    expected_schema: Dict[str, pl.DataType],
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Проверяет схему LazyFrame и разделяет данные на valid и invalid.
    """
    actual_schema = lf.collect_schema()

    missing_columns = []
    type_mismatches = []

    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            missing_columns.append(col_name)
        else:
            actual_type = actual_schema[col_name]

            is_same_base = actual_type.base_type() == expected_type.base_type()
            is_same_numeric_family = (
                actual_type.is_float() and expected_type.is_float()
            ) or (actual_type.is_integer() and expected_type.is_integer())

            if not (is_same_base or is_same_numeric_family):
                type_mismatches.append(
                    f"{col_name}: expected {expected_type}, got {actual_type}"
                )

    if missing_columns or type_mismatches:
        error_msg = f"Schema validation failed for dataset '{dataset}'.\n"
        if missing_columns:
            error_msg += f"Missing mandatory columns: {missing_columns}\n"
        if type_mismatches:
            error_msg += f"Type mismatches: {type_mismatches}\n"

        logger.warning(f"{error_msg.strip()}\nAll rows will be sent to quarantine.")

        empty_lf = lf.filter(pl.lit(False))
        return empty_lf, lf

    new_columns = [col for col in actual_schema if col not in expected_schema]
    if new_columns:
        logger.warning(
            f"Dataset '{dataset}' has new unexpected columns: {new_columns}. They will be ignored."
        )

    empty_lf = lf.filter(pl.lit(False))
    return lf, empty_lf


def filter_by_data_quality(
    lf: pl.LazyFrame,
    validation_rules: Dict[str, Any],
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Применяет бизнес-правила (enums, ranges, custom) и разделяет данные на valid и invalid.
    Не выкидывает исключения, предотвращая потерю NULL-записей до этапа обработки пропусков.
    """
    if not validation_rules:
        return lf, lf.filter(pl.lit(False))

    is_invalid_expr = pl.lit(False)

    # 1. Enums (проверяется только если значение не NULL)
    enums = validation_rules.get("enums", {})
    for col_name, allowed_values in enums.items():
        is_invalid_expr = is_invalid_expr | (
            pl.col(col_name).is_not_null() & ~pl.col(col_name).is_in(allowed_values)
        )

    # 2. Ranges (проверяется только если значение не NULL, сохраняя логику SQL Check Constraints)
    ranges = validation_rules.get("ranges", {})
    for col_name, limits in ranges.items():
        col_expr = pl.col(col_name)
        range_fail_expr = pl.lit(False)
        if "min" in limits:
            range_fail_expr = range_fail_expr | (col_expr < limits["min"])
        if "max" in limits:
            range_fail_expr = range_fail_expr | (col_expr > limits["max"])

        is_invalid_expr = is_invalid_expr | (col_expr.is_not_null() & range_fail_expr)

    # 3. Custom rules (SQL-like. Если возвращает NULL/Unknown - проверка пропускается по стандарту SQL)
    custom_rules = validation_rules.get("custom", [])
    for rule in custom_rules:
        rule_str = rule["rule"]
        sql_rule = (
            rule_str.replace("current_date", "CURRENT_DATE")
            .replace(" is not null", " IS NOT NULL")
            .replace(" is null", " IS NULL")
        )
        try:
            parsed_expr = pl.sql_expr(sql_rule)
            is_invalid_expr = is_invalid_expr | (~parsed_expr).fill_null(False)
        except Exception as e:
            logger.warning(f"Failed to parse custom DQ rule '{rule_str}': {e}")

    is_invalid_expr = is_invalid_expr.fill_null(False)

    valid_lf = lf.filter(~is_invalid_expr)
    invalid_lf = lf.filter(is_invalid_expr)

    return valid_lf, invalid_lf
