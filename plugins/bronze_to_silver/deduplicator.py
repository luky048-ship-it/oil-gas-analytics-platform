from typing import List, Optional

import polars as pl


def deduplicate_dataset(
    lf: pl.LazyFrame, key_columns: List[str], timestamp_column: Optional[str]
) -> pl.LazyFrame:
    """
    Выполняет дедупликацию записей на основе естественных ключей (natural keys).
    Если указан столбец с временной меткой, сохраняется самая последняя запись.
    """
    if not key_columns:
        return lf

    # Сортировка по времени для обеспечения выбора самой актуальной записи при дедупликации
    if timestamp_column:
        lf = lf.sort(timestamp_column)

    # Удаление дубликатов, сохраняя последнюю встреченную строку для каждой комбинации ключей
    return lf.unique(subset=key_columns, keep="last")
