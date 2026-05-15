-- Таблица со справочной информацией о скважинах
CREATE TABLE wells (
    well_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    field_name TEXT,              -- название месторождения
    region TEXT,                  -- регион добычи
    start_date DATE,
    operator TEXT,                -- обслуживающая компания
    status TEXT                   -- active / suspended / maintenance
);

-- Таблица ежедневных производственных показателей
CREATE TABLE production (
    prod_id SERIAL PRIMARY KEY,
    well_id INT REFERENCES wells(well_id),
    date DATE NOT NULL,
    oil_ton NUMERIC(10,2),        -- добыто нефти, тонн
    gas_m3 NUMERIC(12,2),         -- добыто газа, м³
    water_m3 NUMERIC(12,2),       -- добыто воды, м³
    energy_kwh NUMERIC(12,2),     -- энергопотребление
    downtime_hours NUMERIC(5,2),  -- часы простоя
    temperature NUMERIC(5,2),
    pressure NUMERIC(5,2)
);
