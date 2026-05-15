-- Исторические параметры работы оборудования по часам
CREATE TABLE well_telemetry (
    record_id SERIAL PRIMARY KEY,
    well_id INT REFERENCES wells(well_id),
    timestamp TIMESTAMP,
    pump_speed_rpm NUMERIC(8,2),
    pump_current NUMERIC(8,2),
    pressure_in NUMERIC(8,2),
    pressure_out NUMERIC(8,2),
    temperature NUMERIC(5,2),
    vibration NUMERIC(5,2),
    oil_flow_rate NUMERIC(8,2)   -- текущий дебит, тонн/час
);

-- Целевая переменная для модели
CREATE TABLE well_targets (
    well_id INT REFERENCES wells(well_id),
    date DATE,
    daily_oil_ton NUMERIC(10,2)
);
