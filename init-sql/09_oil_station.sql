CREATE TABLE oil_stations (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    oil_flow_per_day FLOAT  -- в тоннах или баррелях
);


INSERT INTO oil_stations (station_name, latitude, longitude, oil_flow_per_day)
VALUES
('Ufa Station', 54.7388, 55.9721, 12500),
('Perm Station', 58.0105, 56.2294, 8700),
('Omsk Station', 54.9893, 73.3682, 15600),
('Tyumen Station', 57.1530, 65.5343, 11200),
('Kazan Station', 55.7963, 49.1088, 9400),
('Samara Station', 53.1959, 50.1008, 10800),
('Saratov Station', 51.5331, 46.0342, 9700),
('Volgograd Station', 48.7080, 44.5133, 8800),
('Yaroslavl Station', 57.6261, 39.8845, 7600),
('Moscow Station', 55.7558, 37.6173, 14300),
('Chelyabinsk Station', 55.1644, 61.4368, 11900),
('Novosibirsk Station', 55.0084, 82.9357, 16700),
('Kurgan Station', 55.4410, 65.3411, 8200),
('Nizhny Novgorod Station', 56.2965, 43.9361, 10200),
('Rostov Station', 47.2357, 39.7015, 9100),
('Krasnodar Station', 45.0355, 38.9753, 8900),
('Voronezh Station', 51.6720, 39.1843, 9800),
('Orenburg Station', 51.7682, 55.0969, 9400),
('Ekaterinburg Station', 56.8389, 60.6057, 13300),
('Astrakhan Station', 46.3497, 48.0408, 8700);
