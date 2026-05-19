import polars as pl
from typing import Dict, Any, Tuple


def validate_critical_rules(
    lf: pl.LazyFrame, rules: Dict[str, Any]
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    ETL-валидатор: проверяет только физические границы (ranges) и справочные значения (enums).
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

    if not conditions:
        return lf, None

    is_invalid = pl.any_horizontal(*conditions)

    valid_lf = lf.filter(~is_invalid)
    invalid_lf = lf.filter(is_invalid).with_columns(
        [pl.lit("PHYSICAL_LIMIT_VIOLATION").alias("_quarantine_reason_code")]
    )

    return valid_lf, invalid_lf
