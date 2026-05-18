from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

import polars as pl
from dq_utils.config import TABLE_CONTRACTS
from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_null_thresholds(
    lf: pl.LazyFrame, dataset: str, thresholds: Dict[str, float]
) -> DQResult:
    """
    Проверяет максимально допустимый процент пустых (NULL) значений в столбцах.
    Все проверки выполняются за один проход по данным.
    """
    if not thresholds:
        return DQResult(
            dataset,
            "Null Thresholds",
            "PASS",
            0,
            0,
            "No thresholds defined",
            datetime.utcnow(),
        )

    # Формирование выражений для подсчета общего количества строк и пустых значений по столбцам
    total_rows_expr = pl.len().alias("total_rows")
    null_exprs = [
        pl.col(c).is_null().sum().alias(f"{c}_nulls") for c in thresholds.keys()
    ]

    # Выполнение всех проверок одним запросом
    result_df = lf.select([total_rows_expr] + null_exprs).collect()

    total_rows = result_df["total_rows"][0]
    if total_rows == 0:
        return DQResult(
            dataset,
            "Null Thresholds",
            "WARNING",
            0,
            0,
            "Empty dataset",
            datetime.utcnow(),
        )

    failed_checks = []
    # Анализ полученных результатов и сравнение с пороговыми значениями
    for col, max_pct in thresholds.items():
        null_count = result_df[f"{col}_nulls"][0]
        actual_pct = (null_count / total_rows) * 100.0
        if actual_pct > max_pct:
            failed_checks.append(f"{col}: {actual_pct:.2f}% > {max_pct}%")

    status = "FAIL" if failed_checks else "PASS"
    message = (
        f"Null thresholds exceeded: {failed_checks}"
        if failed_checks
        else "All null thresholds met."
    )

    return DQResult(
        dataset=dataset,
        validation_type="Null Thresholds",
        status=status,
        failed_rows=len(failed_checks),
        checked_rows=total_rows,
        message=message,
        created_at=datetime.utcnow(),
    )


def validate_duplicate_keys(
    lf: pl.LazyFrame, dataset: str, key_columns: List[str]
) -> DQResult:
    """
    Проверяет уникальность первичных или составных ключей.
    Идентифицирует наличие групп дубликатов.
    """
    if not key_columns:
        return DQResult(
            dataset, "Duplicate Keys", "PASS", 0, 0, "No PK defined", datetime.utcnow()
        )

    # Группировка по ключам и фильтрация групп, где количество записей больше одной
    dup_count_df = (
        lf.group_by(key_columns)
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len().alias("duplicate_groups"))
        .collect()
    )

    duplicate_groups = (
        dup_count_df["duplicate_groups"][0] if not dup_count_df.is_empty() else 0
    )

    status = "FAIL" if duplicate_groups > 0 else "PASS"
    message = (
        f"Found {duplicate_groups} duplicate key groups."
        if duplicate_groups > 0
        else "No duplicates found."
    )

    return DQResult(
        dataset=dataset,
        validation_type="Duplicate Keys",
        status=status,
        failed_rows=duplicate_groups,
        checked_rows=0,
        message=message,
        created_at=datetime.utcnow(),
    )


def validate_business_rules(lf: pl.LazyFrame, dataset: str) -> List[DQResult]:
    """
    Выполняет комплексную проверку доменных ограничений (диапазоны, перечисления)
    за ОДИН проход (single pass) по данным.
    """
    contract = TABLE_CONTRACTS.get(dataset)
    if not contract:
        return []

    exprs = [pl.len().alias("total_rows")]
    check_metadata = []

    # 1. Формирование проверок диапазонов значений (Value Ranges)
    for col, (min_val, max_val) in contract.value_ranges.items():
        conds = []
        if min_val is not None:
            conds.append(pl.col(col) < min_val)
        if max_val is not None:
            conds.append(pl.col(col) > max_val)

        if conds:
            col_alias = f"range_fail_{col}"
            exprs.append(
                pl.any_horizontal(conds).fill_null(False).sum().alias(col_alias)
            )
            check_metadata.append(
                {
                    "alias": col_alias,
                    "type": f"Range Check: {col}",
                    "msg": f"Column {col} out of range ({min_val}, {max_val}).",
                }
            )

    # 2. Формирование проверок по спискам допустимых значений (Enums)
    for col, allowed_values in contract.enums.items():
        col_alias = f"enum_fail_{col}"
        exprs.append(
            (~pl.col(col).is_in(allowed_values)).fill_null(False).sum().alias(col_alias)
        )
        check_metadata.append(
            {
                "alias": col_alias,
                "type": f"Enum Check: {col}",
                "msg": f"Column {col} contains unapproved values.",
            }
        )

    if len(exprs) == 1:
        return []

    # 3. Одновременное выполнение всех накопленных правил через материализацию LazyFrame
    res_df = lf.select(exprs).collect()

    total_rows = res_df["total_rows"][0]
    if total_rows == 0:
        return []

    results = []

    # 4. Преобразование результатов агрегации в объекты отчетов DQResult
    for meta in check_metadata:
        failed_count = res_df[meta["alias"]][0]
        status = "FAIL" if failed_count > 0 else "PASS"
        results.append(
            DQResult(
                dataset=dataset,
                validation_type=meta["type"],
                status=status,
                failed_rows=failed_count,
                checked_rows=total_rows,
                message=meta["msg"] if failed_count > 0 else "Passed",
                created_at=datetime.utcnow(),
            )
        )

    return results
