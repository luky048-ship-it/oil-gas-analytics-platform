# gold_layer/sql_templates.py
CREATE_STAGING_TABLE = """
-- Создаем таблицу только со структурой (без индексов/констрейнтов для скорости I/O)
CREATE TABLE IF NOT EXISTS {staging_table} (LIKE {target_table} EXCLUDING ALL);
TRUNCATE TABLE {staging_table};
"""

DROP_STAGING_TABLE = "DROP TABLE IF EXISTS {staging_table};"

DELETE_PARTITION_FROM_GOLD = """
DELETE FROM {target_table} WHERE partition_date = ANY(%s);
"""

INSERT_FROM_STAGING_TO_GOLD = """
INSERT INTO {target_table}
SELECT * FROM {staging_table}
WHERE partition_date = ANY(%s);
"""
