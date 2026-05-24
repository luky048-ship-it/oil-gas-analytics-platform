# gold_layer/sql_templates.py

# 1. Создаем временную таблицу-клон, исключая индексы, дефолты и автогенераторы
CREATE_STAGING_TABLE = """
CREATE TABLE {staging_table} (
    LIKE {target_table} 
    EXCLUDING IDENTITY 
    EXCLUDING GENERATED 
    EXCLUDING DEFAULTS
);
"""

# 2. Удаляем из Gold-таблицы старые данные за рассчитываемые даты перед записью новых (для идемпотентности)
DELETE_PARTITION_FROM_GOLD = """
DELETE FROM {target_table} WHERE partition_date = ANY(%s);
"""

# 3. Переносим данные, ЯВНО перечисляя только бизнес-колонки.
INSERT_FROM_STAGING_TO_GOLD = """
INSERT INTO {target_table} ({columns}) 
SELECT {columns} 
FROM {staging_table};
"""

# 4. Удаляем временную таблицу после успешного переноса данных
DROP_STAGING_TABLE = "DROP TABLE IF EXISTS {staging_table};"
