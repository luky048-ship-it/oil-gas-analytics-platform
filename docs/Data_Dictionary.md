# Data Dictionary

## wells
Table with reference information about wells.

| Column | Type | Description |
|---|---|---|
| well_id | SERIAL PK | Unique well identifier |
| name | TEXT | Well name |
| field_name | TEXT | Oil field name |
| region | TEXT | Production region |
| start_date | DATE | Production start date |
| operator | TEXT | Operating company |
| status | TEXT | well status (active / suspended / maintenance) |

## production
Table of daily production indicators.

| Column | Type | Description |
|---|---|---|
| prod_id | SERIAL PK | Production record id |
| well_id | INT | Reference to wells |
| date | DATE | Production day |
| oil_ton | NUMERIC(10,2) | Oil produced, tons |
| gas_m3 | NUMERIC(12,2) | Gas produced, m3 |
| water_m3 | NUMERIC(12,2) | Water produced, m3 |
| energy_kwh | NUMERIC(12,2) | Energy consumption |
| downtime_hours | NUMERIC(5,2) | Downtime hours |
| temperature | NUMERIC(5,2) | Average temperature |
| pressure | NUMERIC(5,2) | Average pressure |

## well_telemetry
Hourly historical parameters of equipment operation.

| Column | Type | Description |
|---|---|---|
| record_id | SERIAL PK | Telemetry record id |
| well_id | INT | Reference to wells |
| timestamp | TIMESTAMP | Event timestamp |
| pump_speed_rpm | NUMERIC(8,2) | Pump speed, rpm |
| pump_current | NUMERIC(8,2) | Pump current |
| pressure_in | NUMERIC(8,2) | Inlet pressure |
| pressure_out | NUMERIC(8,2) | Outlet pressure |
| temperature | NUMERIC(5,2) | Temperature |
| vibration | NUMERIC(5,2) | Vibration |
| oil_flow_rate | NUMERIC(8,2) | Flow rate, tons/hour |

## well_targets
Target variable for the model.

| Column | Type | Description |
|---|---|---|
| well_id | INT | Reference to wells |
| date | DATE | Target date |
| daily_oil_ton | NUMERIC(10,2) | Planned oil production |

## pumps
Pump asset registry.

| Column | Type | Description |
|---|---|---|
| pump_id | INT PK | Unique pump id |
| well_id | INT | Linked well |
| type | TEXT | Pump type |
| install_date | DATE | Installation date |
| manufacturer | TEXT | OEM manufacturer |
| model | TEXT | Model identifier |

## pump_sensors
Telemetry from pump sensors.

| Column | Type | Description |
|---|---|---|
| record_id | SERIAL PK | Sensor record id |
| pump_id | INT | Reference to pumps |
| timestamp | TIMESTAMP | Event timestamp |
| temperature | NUMERIC(6,2) | Temperature |
| vibration | NUMERIC(6,2) | Vibration |
| current | NUMERIC(8,2) | Electrical current |
| rpm | NUMERIC(10,2) | Rotational speed |
| pressure | NUMERIC(8,2) | Pressure |

## pump_failures
Pump failure events.

| Column | Type | Description |
|---|---|---|
| failure_id | SERIAL PK | Failure event id |
| pump_id | INT | Affected pump |
| failure_date | TIMESTAMP | Failure timestamp |
| failure_type | TEXT | Failure classification |
| downtime_hours | NUMERIC(5,2) | Outage duration |

## deliveries
Table of routes (warehouse -> client).

| Column | Type | Description |
|---|---|---|
| delivery_id | SERIAL PK | Delivery id |
| date | DATE | Delivery date |
| source | TEXT | Source |
| destination | TEXT | Destination |
| product_type | TEXT | Product type |
| volume_ton | NUMERIC(10,2) | Volume |
| cost_usd | NUMERIC(10,2) | Cost |
| delay_hours | NUMERIC(6,2) | Delay hours |
| distance_km | NUMERIC(8,2) | Distance |
| weather_conditions | TEXT | Weather |
| driver_id | INT | Driver id |
| vehicle_id | INT | Vehicle id |

## drivers
Drivers table.

| Column | Type | Description |
|---|---|---|
| driver_id | SERIAL PK | Driver id |
| name | TEXT | Driver name |
| experience_years | INT | Years of experience |
| region | TEXT | Region |

## vehicles
Transport table.

| Column | Type | Description |
|---|---|---|
| vehicle_id | SERIAL PK | Vehicle id |
| plate_number | TEXT | Plate number |
| capacity_ton | NUMERIC(8,2) | Capacity (tons) |
| fuel_type | TEXT | Fuel type |

## oil_stations
Table of oil pumping stations.

| Column | Type | Description |
|---|---|---|
| station_id | SERIAL PK | Station identifier |
| station_name | VARCHAR(100) | Station name |
| latitude | FLOAT | Latitude |
| longitude | FLOAT | Longitude |
| oil_flow_per_day | FLOAT | Oil flow per day |

## etl_metadata.loaded_partitions
Service table for tracking incremental data loading.

| Column | Type | Description |
|---|---|---|
| table_name | TEXT PK | Loaded table name |
| partition_date | DATE PK | Partition date |
| status | TEXT | Loading status |
| loaded_at | TIMESTAMPTZ | Start time of loading |
| updated_at | TIMESTAMPTZ | Time of last update |
| dag_run_id | TEXT | DAG run ID |

## etl_metadata.marts_loaded_partitions
Service table for tracking data mart loading.

| Column | Type | Description |
|---|---|---|
| mart_name | VARCHAR(100) PK | Mart name |
| partition_date | DATE PK | Partition date |
| dag_run_id | VARCHAR(250) | DAG run ID |
| loaded_at | TIMESTAMP | Loading time |

## gold.mart_production
Main production mart (day x well).

| Column | Type | Description |
|---|---|---|
| mart_id | BIGSERIAL PK | Record identifier |
| well_id | INTEGER | Well ID |
| date | DATE | Measurement date |
| oil_ton | NUMERIC(12,3) | Oil Ton |
| gas_m3 | NUMERIC(14,2) | Gas m3 |
| water_m3 | NUMERIC(14,2) | Water m3 |
| energy_kwh | NUMERIC(14,2) | Energy |
| downtime_hours | NUMERIC(6,2) | Downtime hours |
| avg_temperature | NUMERIC(6,2) | Average temperature |
| avg_pressure | NUMERIC(8,2) | Average pressure |
| avg_pump_speed_rpm | NUMERIC(10,2) | Average RPM |
| avg_oil_flow_rate | NUMERIC(10,3) | Average flow rate |
| max_vibration | NUMERIC(6,2) | Max vibration |
| daily_target_ton | NUMERIC(12,3) | Target |
| production_efficiency | NUMERIC(8,4) | Production efficiency |
| downtime_pct | NUMERIC(6,3) | Downtime percentage |
| load_timestamp | TIMESTAMP | Loading time |
| partition_date | DATE | Partition date |

## gold.mart_well_kpi
KPI per well per date.

| Column | Type | Description |
|---|---|---|
| well_id | INTEGER PK | Well ID |
| date | DATE PK | Date |
| avg_daily_oil | NUMERIC(12,3) | Average daily oil |
| total_oil | NUMERIC(14,3) | Total oil |
| avg_downtime_pct | NUMERIC(6,3) | Average downtime pct |
| avg_efficiency | NUMERIC(8,4) | Average efficiency |
| best_day_oil | NUMERIC(12,3) | Best day oil |
| worst_day_oil | NUMERIC(12,3) | Worst day oil |
| production_rank | INTEGER | Production rank |
| performance_group | TEXT | Performance group |
| load_timestamp | TIMESTAMP | Loading time |
| partition_date | DATE | Partition date |

## gold.mart_failures
Failure, anomaly, and predictive maintenance mart.

| Column | Type | Description |
|---|---|---|
| record_id | BIGSERIAL PK | Record identifier |
| pump_id | INTEGER | Pump ID |
| well_id | INTEGER | Well ID |
| date | DATE | Date |
| timestamp | TIMESTAMP | Event timestamp |
| temperature | NUMERIC(6,2) | Temperature |
| vibration | NUMERIC(6,2) | Vibration |
| current | NUMERIC(8,2) | Electrical current |
| rpm | NUMERIC(10,2) | Rotational speed |
| pressure | NUMERIC(8,2) | Pressure |
| vibration_zscore | NUMERIC(6,3) | Vibration Z-score |
| temperature_zscore | NUMERIC(6,3) | Temperature Z-score |
| is_anomaly | BOOLEAN | Anomaly indicator |
| anomaly_reason | TEXT[] | Anomaly reasons |
| failure_type | TEXT | Failure type |
| is_failure | BOOLEAN | Failure indicator |
| risk_score | NUMERIC(5,4) | Risk score |
| failure_probability | NUMERIC(5,4) | Failure probability |
| load_timestamp | TIMESTAMP | Loading time |
| partition_date | DATE | Partition date |

## gold.mart_logistics
Logistics and deliveries mart.

| Column | Type | Description |
|---|---|---|
| delivery_id | BIGINT PK | Delivery ID |
| date | DATE | Delivery date |
| source | TEXT | Source |
| destination | TEXT | Destination |
| product_type | TEXT | Product type |
| volume_ton | NUMERIC(12,3) | Volume |
| cost_usd | NUMERIC(14,2) | Cost |
| delay_hours | NUMERIC(8,2) | Delay hours |
| distance_km | NUMERIC(10,2) | Distance |
| weather_conditions | TEXT | Weather |
| driver_id | INTEGER | Driver ID |
| driver_name | TEXT | Driver name |
| experience_years | INTEGER | Experience |
| vehicle_id | INTEGER | Vehicle ID |
| plate_number | TEXT | Plate number |
| capacity_ton | NUMERIC(8,2) | Capacity |
| fuel_type | TEXT | Fuel type |
| cost_per_km | NUMERIC(10,2) | Cost per km |
| cost_per_ton | NUMERIC(10,2) | Cost per ton |
| delay_flag | BOOLEAN | Delay indicator |
| weather_impact | TEXT | Weather impact |
| load_timestamp | TIMESTAMP | Loading time |
| partition_date | DATE | Partition date |
