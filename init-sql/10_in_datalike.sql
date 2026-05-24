-- Метаданные для инкрементальной загрузки данных
CREATE SCHEMA IF NOT EXISTS etl_metadata;


-- --------------------------------------------------------------
-- Bronze Layer
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_metadata.loaded_partitions (
    table_name VARCHAR(100) NOT NULL,
    partition_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    dag_run_id VARCHAR(250),
    file_path VARCHAR(1000),
    row_count BIGINT DEFAULT 0,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, partition_date)
);

-- --------------------------------------------------------------
-- Silver Layer
-- --------------------------------------------------------------


CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_watermarks (
    dataset VARCHAR(100) NOT NULL PRIMARY KEY,
    last_processed_watermark TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_executions (
    dataset VARCHAR(100) NOT NULL,
    partition_date DATE NOT NULL,
    processed_rows BIGINT NOT NULL,
    quarantined_rows BIGINT NOT NULL,
    execution_time_sec DOUBLE PRECISION NOT NULL,
    watermark TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, partition_date)
);

-- --------------------------------------------------------------
-- Gold Layer
-- --------------------------------------------------------------
    CREATE SCHEMA IF NOT EXISTS staging;

    -- Таблица для отслеживания вотермарков gold
    CREATE TABLE IF NOT EXISTS etl_metadata.marts_loaded_partitions (
      mart_name       VARCHAR(100) NOT NULL,
      partition_date  DATE NOT NULL,
      dag_run_id      VARCHAR(250),
      loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (mart_name, partition_date)
      );


      COMMENT ON TABLE etl_metadata.marts_loaded_partitions IS 'Метаданные загрузки витрин (Gold layer)';

