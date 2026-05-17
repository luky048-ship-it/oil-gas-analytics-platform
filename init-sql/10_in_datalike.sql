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
