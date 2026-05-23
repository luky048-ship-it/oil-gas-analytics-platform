-- =============================================
-- GOLD LAYER — Создание схемы и витрин
-- =============================================

-- 1. Создание схемы
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================
-- 2. mart_production
-- =============================================
CREATE TABLE IF NOT EXISTS gold.mart_production (
    mart_id                 BIGSERIAL PRIMARY KEY,
    well_id                 INTEGER NOT NULL,
    date                    DATE NOT NULL,
    
    -- Основные производственные метрики
    oil_ton                 NUMERIC(12,3),
    gas_m3                  NUMERIC(14,2),
    water_m3                NUMERIC(14,2),
    energy_kwh              NUMERIC(14,2),
    downtime_hours          NUMERIC(6,2),
    
    -- Агрегированная телеметрия
    avg_temperature         NUMERIC(6,2),
    avg_pressure            NUMERIC(8,2),
    avg_pump_speed_rpm      NUMERIC(10,2),
    avg_oil_flow_rate       NUMERIC(10,3),
    max_vibration           NUMERIC(6,2),
    
    -- KPI
    daily_target_ton        NUMERIC(12,3),
    production_efficiency   NUMERIC(8,4),      -- oil_ton / target
    downtime_pct            NUMERIC(6,3),       -- процент простоя
    
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    partition_date          DATE GENERATED ALWAYS AS (date) STORED
);

COMMENT ON TABLE gold.mart_production IS 'Основная производственная витрина';

-- Индексы
CREATE INDEX IF NOT EXISTS idx_mart_prod_well_date ON gold.mart_production (well_id, date);
CREATE INDEX IF NOT EXISTS idx_mart_prod_date ON gold.mart_production (date);


-- =============================================
-- 3. mart_well_kpi
-- =============================================
CREATE TABLE IF NOT EXISTS gold.mart_well_kpi (
    well_id                 INTEGER NOT NULL,
    date                    DATE NOT NULL,
    
    avg_daily_oil           NUMERIC(12,3),
    total_oil               NUMERIC(14,3),
    avg_downtime_pct        NUMERIC(6,3),
    avg_efficiency          NUMERIC(8,4),
    best_day_oil            NUMERIC(12,3),
    worst_day_oil           NUMERIC(12,3),
    
    production_rank         INTEGER,
    performance_group       TEXT,                    -- 'Top', 'Good', 'Average', 'Poor'
    
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    partition_date          DATE GENERATED ALWAYS AS (date) STORED,
    
    PRIMARY KEY (well_id, date)
);

COMMENT ON TABLE gold.mart_well_kpi IS 'KPI по скважинам (эффективность, ранжирование)';

CREATE INDEX IF NOT EXISTS idx_mart_kpi_date ON gold.mart_well_kpi (date);
CREATE INDEX IF NOT EXISTS idx_mart_kpi_group ON gold.mart_well_kpi (performance_group);


-- =============================================
-- 4. mart_failures
-- =============================================
CREATE TABLE IF NOT EXISTS gold.mart_failures (
    record_id               BIGSERIAL PRIMARY KEY,
    pump_id                 INTEGER NOT NULL,
    well_id                 INTEGER NOT NULL,
    date                    DATE NOT NULL,
    timestamp               TIMESTAMP,
    
    -- Сенсоры
    temperature             NUMERIC(6,2),
    vibration               NUMERIC(6,2),
    current                 NUMERIC(8,2),
    rpm                     NUMERIC(10,2),
    pressure                NUMERIC(8,2),
    
    -- Аномалии
    vibration_zscore        NUMERIC(6,3),
    temperature_zscore      NUMERIC(6,3),
    is_anomaly              BOOLEAN DEFAULT FALSE,
    anomaly_reason          TEXT[],
    
    -- Отказы
    failure_type            TEXT,
    is_failure              BOOLEAN DEFAULT FALSE,
    
    -- Предиктивная аналитика
    risk_score              NUMERIC(5,4),           -- 0.0000 - 1.0000
    failure_probability     NUMERIC(5,4),
    
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    partition_date          DATE GENERATED ALWAYS AS (date) STORED
);

COMMENT ON TABLE gold.mart_failures IS 'Аналитика отказов, аномалий и рисков оборудования';

CREATE INDEX IF NOT EXISTS idx_mart_fail_pump_date ON gold.mart_failures (pump_id, date);
CREATE INDEX IF NOT EXISTS idx_mart_fail_anomaly ON gold.mart_failures (is_anomaly);
CREATE INDEX IF NOT EXISTS idx_mart_fail_risk ON gold.mart_failures (risk_score DESC);


-- =============================================
-- 5. mart_logistics
-- =============================================
CREATE TABLE IF NOT EXISTS gold.mart_logistics (
    delivery_id             BIGINT PRIMARY KEY,
    date                    DATE NOT NULL,
    
    source                  TEXT,
    destination             TEXT,
    product_type            TEXT,
    volume_ton              NUMERIC(12,3),
    cost_usd                NUMERIC(14,2),
    delay_hours             NUMERIC(8,2),
    distance_km             NUMERIC(10,2),
    weather_conditions      TEXT,
    
    -- Обогащение справочниками
    driver_id               INTEGER,
    driver_name             TEXT,
    experience_years        INTEGER,
    vehicle_id              INTEGER,
    plate_number            TEXT,
    capacity_ton            NUMERIC(8,2),
    fuel_type               TEXT,
    
    -- Расчётные показатели
    cost_per_km             NUMERIC(10,2),
    cost_per_ton            NUMERIC(10,2),
    delay_flag              BOOLEAN,
    weather_impact          TEXT,                    -- 'high', 'medium', 'low'
    
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    partition_date          DATE GENERATED ALWAYS AS (date) STORED
);

COMMENT ON TABLE gold.mart_logistics IS 'Витрина логистики и поставок';

CREATE INDEX IF NOT EXISTS idx_mart_log_date ON gold.mart_logistics (date);
CREATE INDEX IF NOT EXISTS idx_mart_log_driver ON gold.mart_logistics (driver_id);
CREATE INDEX IF NOT EXISTS idx_mart_log_weather ON gold.mart_logistics (weather_conditions);


-- =============================================
-- PREDICTIVE ANALYTICS LAYER (ML Marts)
-- =============================================

-- 1. Предсказание дебита скважин
CREATE TABLE IF NOT EXISTS gold.ml_flow_predictions (
    well_id INTEGER NOT NULL,
    date DATE NOT NULL,
    actual_oil_ton NUMERIC(12,3),
    predicted_oil_ton NUMERIC(12,3),
    prediction_error NUMERIC(12,3),
    model_version TEXT,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (well_id, date)
);

COMMENT ON TABLE gold.ml_flow_predictions IS 'Результаты ML-прогнозирования дебита скважин (Inference)';

-- 2. Предсказание отказов и аномалий насосов
CREATE TABLE IF NOT EXISTS gold.ml_pump_predictions (
    record_id BIGINT PRIMARY KEY,
    pump_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_anomaly_ml BOOLEAN NOT NULL,
    risk_score NUMERIC(5,4) NOT NULL,
    failure_probability NUMERIC(5,4) NOT NULL,
    model_version TEXT,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.ml_pump_predictions IS 'ML-скоринг аномалий и вероятности отказов насосов (Isolation Forest & Random Forest)';

CREATE INDEX IF NOT EXISTS idx_ml_pump_timestamp ON gold.ml_pump_predictions (timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_pump_anomaly ON gold.ml_pump_predictions (is_anomaly_ml);
