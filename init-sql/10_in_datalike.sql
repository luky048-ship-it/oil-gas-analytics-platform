-- Метаданные для инкрементальной загрузки данных
CREATE SCHEMA IF NOT EXISTS etl_metadata;
CREATE TABLE IF NOT EXISTS etl_metadata.loaded_partitions (
    table_name     TEXT NOT NULL,
    partition_date DATE NOT NULL,
    status         TEXT NOT NULL DEFAULT 'processing',
    loaded_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dag_run_id     TEXT,
    PRIMARY KEY (table_name, partition_date)
);
CREATE TABLE IF NOT EXISTS etl_metadata.marts_loaded_partitions (
    mart_name VARCHAR(100) NOT NULL,
    partition_date DATE NOT NULL,
    dag_run_id VARCHAR(250),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mart_name, partition_date)
);
