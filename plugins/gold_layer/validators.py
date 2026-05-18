import polars as pl
import logging
from gold_layer.config import MART_CONTRACTS

logger = logging.getLogger(__name__)

def validate_business_readiness(lf: pl.LazyFrame, mart_name: str) -> pl.LazyFrame:
    """
    Выполняет проверку готовности данных на соответствие бизнес-логике.
    Логирует предупреждения при обнаружении аномалий (например, отрицательные показатели добычи),
    но не прерывает выполнение процесса.
    """
    contract = MART_CONTRACTS.get(mart_name)
    if not contract:
        return lf

    rules = contract.business_rules

    # Пример проверки: выявление отрицательных значений добычи нефти
    if "min_oil_ton" in rules and "oil_ton" in lf.collect_schema().names():
        neg_count = lf.filter(pl.col("oil_ton") < rules["min_oil_ton"]).collect().height
        if neg_count > 0:
            logger.warning(f"[{mart_name}] Found {neg_count} rows with oil_ton < {rules['min_oil_ton']}")

    return lf

def validate_mart_before_publish(df: pl.DataFrame, mart_name: str):
    """
    Выполняет строгую валидацию материализованной витрины данных перед её публикацией в базу.
    Прерывает выполнение процесса (raise ValueError) при нарушении критических требований контракта.
    """
    contract = MART_CONTRACTS.get(mart_name)
    if not contract:
        logger.warning(f"No contract found for mart {mart_name}")
        return

    # 1. Проверка на отсутствие пустых (NULL) значений в критически важных столбцах
    for col in contract.critical_columns:
        if col in df.columns:
            null_count = df.filter(pl.col(col).is_null()).height
            if null_count > 0:
                raise ValueError(f"CRITICAL: {mart_name} has {null_count} NULLs in critical column {col}")

    # 2. Проверка на наличие данных в результирующем наборе
    if df.height == 0:
        logger.warning(f"[{mart_name}] No rows to publish for this batch.")

    logger.info(f"[{mart_name}] Validation passed for {df.height} rows.")
