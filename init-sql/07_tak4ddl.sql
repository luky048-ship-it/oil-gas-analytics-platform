-- Таблица маршрутов (склад → клиент)
CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    date DATE,
    source TEXT,
    destination TEXT,
    product_type TEXT,           -- дизель, бензин и т.д.
    volume_ton NUMERIC(10,2),
    cost_usd NUMERIC(10,2),
    delay_hours NUMERIC(6,2),
    distance_km NUMERIC(8,2),
    weather_conditions TEXT,
    driver_id INT,
    vehicle_id INT
);

-- Таблица водителей
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    name TEXT,
    experience_years INT,
    region TEXT
);

-- Таблица транспорта
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    plate_number TEXT,
    capacity_ton NUMERIC(8,2),
    fuel_type TEXT
);
