import logging

import polars as pl
from adbc_driver_postgresql import dbapi as adbc_dbapi

from gold_layer.connections import get_postgres_uri, get_psycopg2_conn
from gold_layer.constants import GOLD_SCHEMA, STAGING_SCHEMA
from gold_layer.sql_templates import (CREATE_STAGING_TABLE,
                                      DELETE_PARTITION_FROM_GOLD,
                                      DROP_STAGING_TABLE)

logger = logging.getLogger(__name__)


def _get_target_table_metadata(mart_name: str):
    """
    Получает метаданные колонок из системных таблиц Postgres для выравнивания типов и порядка данных.
    Исключает генерируемые колонки и автоинкременты для предотвращения ошибок при вставке.
    """
    query = """
        SELECT 
            column_name, 
            data_type, 
            numeric_precision, 
            numeric_scale
        FROM information_schema.columns 
        WHERE table_schema = %s 
          AND table_name = %s
          AND is_generated = 'NEVER'
          AND (column_default IS NULL OR column_default NOT LIKE 'nextval%%')
        ORDER BY ordinal_position;
    """
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (GOLD_SCHEMA, mart_name))
            return cur.fetchall()


def write_staging_mart(df: pl.DataFrame, mart_name: str) -> str:
    """
    Записывает рассчитанную витрину (DataFrame) во временную staging-таблицу в базе данных.
    Выполняет приведение типов (precision/scale alignment) и гарантирует порядок столбцов,
    необходимый для корректной работы драйвера ADBC.
    """
    staging_table = f"{STAGING_SCHEMA}.stg_{mart_name}"
    target_table = f"{GOLD_SCHEMA}.{mart_name}"
    uri = get_postgres_uri()

    # 1. Получение метаданных целевой таблицы для синхронизации структуры
    db_columns = _get_target_table_metadata(mart_name)

    cast_exprs = []
    final_column_order = []

    # Подготовка выражений для приведения типов данных Polars к типам Postgres
    for col_name, dtype, precision, scale in db_columns:
        if col_name in df.columns:
            final_column_order.append(col_name)

            if dtype in ("numeric", "decimal") and scale is not None:
                logger.debug(f"Aligning {col_name} to Decimal({precision}, {scale})")
                cast_exprs.append(pl.col(col_name).cast(pl.Decimal(precision, scale)))

            elif "int" in dtype:
                target_int = pl.Int32 if dtype == "integer" else pl.Int64
                cast_exprs.append(pl.col(col_name).cast(target_int))

            elif dtype in ("double precision", "real"):
                cast_exprs.append(pl.col(col_name).cast(pl.Float64))

    # Применение трансформаций и упорядочивание столбцов
    df_aligned = df.with_columns(cast_exprs).select(final_column_order)

    # 2. Создание staging таблицы на основе структуры целевой витрины
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                CREATE_STAGING_TABLE.format(
                    staging_table=staging_table, target_table=target_table
                )
            )
        conn.commit()

    # 3. Высокопроизводительная вставка данных через ADBC
    arrow_table = df_aligned.to_arrow()
    with adbc_dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.adbc_ingest(staging_table, arrow_table, mode="append")

    logger.info(f"Staged {len(df_aligned)} rows for {mart_name} (aligned by DB schema)")
    return staging_table


def atomic_partition_overwrite(
    mart_name: str, staging_table: str, partition_dates: list
):
    """
    Выполняет атомарную перезапись разделов (partitions) в целевой таблице Gold-слоя.
    Сначала удаляет старые данные за указанные даты, затем вставляет новые из staging-таблицы.
    """
    target_table = f"{GOLD_SCHEMA}.{mart_name}"

    db_columns = _get_target_table_metadata(mart_name)
    col_names_quoted = [f'"{c[0]}"' for c in db_columns]
    col_list_str = ", ".join(col_names_quoted)

    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            for dt in partition_dates:
                logger.info(f"Atomic update for {mart_name} partition: {dt}")

                # Удаление существующих данных за дату раздела
                cur.execute(
                    DELETE_PARTITION_FROM_GOLD.format(target_table=target_table), (dt,)
                )

                # Вставка новых данных с явным перечислением столбцов
                insert_sql = f"""
                    INSERT INTO {target_table} ({col_list_str})
                    SELECT {col_list_str} FROM {staging_table}
                    WHERE date = %s
                """
                cur.execute(insert_sql, (dt,))
        conn.commit()
    logger.info(f"Successfully updated {target_table} for dates: {partition_dates}")


def cleanup_staging(staging_table: str):
    """Удаляет временную staging-таблицу после успешного переноса данных в витрину."""
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DROP_STAGING_TABLE.format(staging_table=staging_table))
        conn.commit()
    logger.debug(f"Staging table {staging_table} cleaned up.")
