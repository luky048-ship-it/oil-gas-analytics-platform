# SQL Templates for Staging and Atomic Overwrite

CREATE_STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS {staging_table} (LIKE {target_table} INCLUDING ALL);
TRUNCATE TABLE {staging_table};
"""

DROP_STAGING_TABLE = "DROP TABLE IF EXISTS {staging_table};"

DELETE_PARTITION_FROM_GOLD = """
DELETE FROM {target_table} WHERE partition_date = %s;
"""

INSERT_FROM_STAGING_TO_GOLD = """
INSERT INTO {target_table}
SELECT * FROM {staging_table}
WHERE partition_date = %s;
"""

# Validation Queries
CHECK_NULLS = "SELECT count(*) FROM {staging_table} WHERE {column} IS NULL;"
CHECK_DUPLICATES = "SELECT {pk_cols}, count(*) FROM {staging_table} GROUP BY {pk_cols} HAVING count(*) > 1;"
CHECK_ROW_COUNT = "SELECT count(*) FROM {staging_table};"
