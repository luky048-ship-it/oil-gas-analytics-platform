# -----------------------------------------------------------------------------
# SQL ШАБЛОНЫ ДЛЯ СОРЕВНОВАТЕЛЬНОЙ ЗАГРУЗКИ И ОБНОВЛЕНИЯ ДАННЫХ
# -----------------------------------------------------------------------------

# Создание временной таблицы на основе структуры целевой витрины
CREATE_STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS {staging_table} (LIKE {target_table} INCLUDING ALL);
TRUNCATE TABLE {staging_table};
"""

# Удаление временной таблицы
DROP_STAGING_TABLE = "DROP TABLE IF EXISTS {staging_table};"

# Удаление данных за конкретную дату перед вставкой новых (для идемпотентности)
DELETE_PARTITION_FROM_GOLD = """
DELETE FROM {target_table} WHERE partition_date = %s;
"""

# Перенос данных из staging в целевую таблицу (позиционный)
INSERT_FROM_STAGING_TO_GOLD = """
INSERT INTO {target_table}
SELECT * FROM {staging_table}
WHERE partition_date = %s;
"""

# -----------------------------------------------------------------------------
# ЗАПРОСЫ ДЛЯ ВАЛИДАЦИИ ДАННЫХ ПЕРЕД ПУБЛИКАЦИЕЙ
# -----------------------------------------------------------------------------

# Проверка наличия пустых значений в критических столбцах
CHECK_NULLS = "SELECT count(*) FROM {staging_table} WHERE {column} IS NULL;"

# Проверка на наличие дубликатов по первичным ключам
CHECK_DUPLICATES = "SELECT {pk_cols}, count(*) FROM {staging_table} GROUP BY {pk_cols} HAVING count(*) > 1;"

# Подсчет общего количества строк
CHECK_ROW_COUNT = "SELECT count(*) FROM {staging_table};"
