-- =============================================
-- STAGING LAYER — Для временных данных Gold
-- =============================================

CREATE SCHEMA IF NOT EXISTS staging;

-- Таблица для отслеживания вотермарков (если еще не создана в других скриптах)
CREATE TABLE IF NOT EXISTS etl_metadata.marts_loaded_partitions (
    mart_name       VARCHAR(100) NOT NULL,
    partition_date  DATE NOT NULL,
    dag_run_id      VARCHAR(250),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mart_name, partition_date)
);

COMMENT ON TABLE etl_metadata.marts_loaded_partitions IS 'Метаданные загрузки витрин (Gold layer)';
