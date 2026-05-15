-- Таблицы метаданных для DQ и оркестрации
CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_watermarks (
    dataset VARCHAR(100) PRIMARY KEY,
    last_processed_watermark TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_metadata.dq_validation_results (
    dataset VARCHAR(100),
    validation_type VARCHAR(100),
    partition_date DATE,
    execution_date DATE,
    status VARCHAR(20),
    failed_rows BIGINT,
    checked_rows BIGINT,
    message TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, validation_type, partition_date, execution_date)
);

CREATE TABLE IF NOT EXISTS etl_metadata.dq_pipeline_runs (
    dataset VARCHAR(100),
    partition_date DATE,
    execution_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, partition_date)
);

CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_executions (
    dataset VARCHAR(100),
    partition_date DATE,
    processed_rows BIGINT,
    quarantined_rows BIGINT,
    execution_time_sec DOUBLE PRECISION,
    watermark TIMESTAMP,
    status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dataset, partition_date)
);

CREATE TABLE IF NOT EXISTS etl_metadata.dq_quarantine_registry (
    dataset VARCHAR(100),
    partition_date DATE,
    quarantine_path TEXT,
    reason_code VARCHAR(50),
    row_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, partition_date, reason_code)
);
