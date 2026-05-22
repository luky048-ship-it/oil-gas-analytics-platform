-- Метаданные для инкрементальной загрузки данных
CREATE SCHEMA IF NOT EXISTS etl_metadata;
CREATE TABLE IF NOT EXISTS etl_metadata.loaded_partitions (
    table_name     TEXT NOT NULL,
    partition_date DATE NOT NULL,
    status         TEXT NOT NULL DEFAULT 'processing',
    loaded_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dag_run_id     TEXT,
    file_path      TEXT,
    row_count BIGINT,
    PRIMARY KEY (table_name, partition_date)
);


CREATE TABLE IF NOT EXISTS etl_metadata.marts_loaded_partitions (
    mart_name VARCHAR(100) NOT NULL,
    partition_date DATE NOT NULL,
    dag_run_id VARCHAR(250),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mart_name, partition_date)
);


CREATE TABLE IF NOT EXISTS etl_metadata.dq_validation_results (
    dataset VARCHAR(255),
    validation_type VARCHAR(255),
    partition_date DATE,
    execution_date DATE,
    status VARCHAR(50),
    failed_rows BIGINT,
    checked_rows BIGINT,
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (dataset, validation_type, partition_date, execution_date)
);

CREATE TABLE IF NOT EXISTS etl_metadata.dq_pipeline_runs (
    dataset VARCHAR(255),
    partition_date DATE,
    execution_date DATE,
    status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (dataset, partition_date)
);


-- --------------------------------------------------------------
-- Silver Layer
-- --------------------------------------------------------------


-- Используется для защиты от дублей при догрузке данных (late-arriving data)
CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_watermarks (
    dataset VARCHAR(255) PRIMARY KEY,
    last_processed_watermark TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Таблица для аудита запусков Silver пайплайна
-- Хранит метрики: сколько строк обработано, сколько ушло в карантин, время выполнения
CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_executions (
    dataset VARCHAR(255) NOT NULL,
    partition_date DATE NOT NULL,
    processed_rows BIGINT DEFAULT 0,
    quarantined_rows BIGINT DEFAULT 0,
    execution_time_sec NUMERIC(10, 2),
    watermark TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'SUCCESS',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (dataset, partition_date)
);

-- Индексы для ускорения выборок при генерации отчетов о качестве данных
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_date 
    ON etl_metadata.pipeline_executions(partition_date);
    
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status 
    ON etl_metadata.pipeline_executions(status);

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

