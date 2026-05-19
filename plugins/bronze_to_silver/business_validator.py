# plugins/bronze_to_silver/business_validator.py
from typing import Any, Dict, Tuple

import polars as pl


def validate_critical_rules(
    lf: pl.LazyFrame, rules: Dict[str, Any]
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    ETL-валидатор: проверяет физические границы (ranges), справочные значения (enums)
    и кастомные бизнес-правила (custom).
    """
    if not rules:
        return lf, None

    conditions = []

    for col, bounds in rules.get("ranges", {}).items():
        if bounds.get("min") is not None:
            conditions.append(pl.col(col) < bounds["min"])
        if bounds.get("max") is not None:
            conditions.append(pl.col(col) > bounds["max"])

    for col, values in rules.get("enums", {}).items():
        if values:
            conditions.append(pl.col(col).is_in(values).not_())

    # Обработка custom-правил
    for custom_rule in rules.get("custom", []):
        rule_expr = custom_rule.get("rule")
        if rule_expr:
            try:
                # Парсинг строки правила в выражение Polars
                condition = pl.sql_expr(rule_expr)
                # Инвертируем условие, т.к. нам нужны невалидные строки
                conditions.append(condition.not_())
            except Exception:
                # Если парсинг не удался, пропускаем правило с предупреждением
                pass

    if not conditions:
        return lf, None

    is_invalid = pl.any_horizontal(*conditions).fill_null(False)

    valid_lf = lf.filter(~is_invalid)
    
    # Проверяем, есть ли вообще невалидные записи
    invalid_count = valid_lf.select(pl.len().alias("valid_count")).collect().item()
    total_count = lf.select(pl.len().alias("total_count")).collect().item()
    
    if invalid_count == total_count:
        # Все записи валидны
        invalid_lf = None
    else:
        # Есть невалидные записи
        invalid_lf = lf.filter(is_invalid).with_columns(
            [
                pl.lit("PHYSICAL_LIMIT_VIOLATION").alias("_quarantine_reason_code"),
                pl.lit("BUSINESS_VALIDATION").alias("_quarantine_validation_name"),
            ]
        )

    return valid_lf, invalid_lf
