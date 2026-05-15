-- Справочник насосов
CREATE TABLE pumps (
    pump_id SERIAL PRIMARY KEY,
    well_id INT REFERENCES wells(well_id),
    type TEXT,
    install_date DATE,
    manufacturer TEXT,
    model TEXT
);

-- Потоковые данные о состоянии насосов (пример: каждую минуту)
CREATE TABLE pump_sensors (
    record_id SERIAL PRIMARY KEY,
    pump_id INT REFERENCES pumps(pump_id),
    timestamp TIMESTAMP,
    temperature NUMERIC(5,2),
    vibration NUMERIC(5,2),
    current NUMERIC(8,2),
    rpm NUMERIC(8,2),
    pressure NUMERIC(8,2)
);

-- Факты отказов насосов
CREATE TABLE pump_failures (
    failure_id SERIAL PRIMARY KEY,
    pump_id INT REFERENCES pumps(pump_id),
    failure_date TIMESTAMP,
    failure_type TEXT,             -- механический, перегрев и т. д.
    downtime_hours NUMERIC(5,2)
);
