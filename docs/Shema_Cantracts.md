# Schema Contracts (Sychronized with SQL Source of Truth)

---

# 1. Introduction
This document defines the physical schema, data types, and quality constraints for the Oil & Gas Analytics Platform.

---

# 2. Global Standards
- **Charset:** UTF-8
- **Timezone:** UTC
- **Precision:** Synchronized with PostgreSQL DDL.

---

# 3. Table Contracts

## 5.1 wells
| Column | Type | Nullable | Description |
|---|---|---|---|
| well_id | serial | NO | unique well identifier (PK) |
| name | text | NO | operational well name |
| field_name | text | YES | oil field name |
| region | text | YES | production region |
| start_date | date | YES | production start date |
| operator | text | YES | operating company |
| status | text | YES | well status (active / suspended / maintenance) |

## 5.2 production
| Column | Type | Nullable | Description |
|---|---|---|---|
| prod_id | serial | NO | production record id (PK) |
| well_id | integer | YES | well reference (FK) |
| date | date | NO | production day |
| oil_ton | numeric(10,2) | YES | oil production in tons |
| gas_m3 | numeric(12,2) | YES | gas production in m3 |
| water_m3 | numeric(12,2) | YES | water production |
| energy_kwh | numeric(12,2) | YES | consumed energy |
| downtime_hours | numeric(5,2) | YES | downtime duration |
| temperature | numeric(5,2) | YES | average temperature |
| pressure | numeric(5,2) | YES | average pressure |

## 5.3 well_telemetry
| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | serial | NO | telemetry record id (PK) |
| well_id | integer | YES | well reference (FK) |
| timestamp | timestamp | YES | event timestamp |
| pump_speed_rpm | numeric(8,2) | YES | pump speed |
| pump_current | numeric(8,2) | YES | pump current |
| pressure_in | numeric(8,2) | YES | inlet pressure |
| pressure_out | numeric(8,2) | YES | outlet pressure |
| temperature | numeric(5,2) | YES | temperature |
| vibration | numeric(5,2) | YES | vibration |
| oil_flow_rate | numeric(8,2) | YES | flow rate |

## 5.4 well_targets
| Column | Type | Nullable | Description |
|---|---|---|---|
| well_id | integer | YES | well reference (FK) |
| date | date | YES | target date |
| daily_oil_ton | numeric(10,2) | YES | planned production |

## 5.5 pumps
| Column | Type | Nullable | Description |
|---|---|---|---|
| pump_id | serial | NO | unique pump id (PK) |
| well_id | integer | YES | linked well (FK) |
| type | text | YES | pump type |
| install_date | date | YES | installation date |
| manufacturer | text | YES | manufacturer |
| model | text | YES | model identifier |

## 5.6 pump_sensors
| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | serial | NO | sensor record id (PK) |
| pump_id | integer | YES | pump reference (FK) |
| timestamp | timestamp | YES | event timestamp |
| temperature | numeric(6,2) | YES | temperature |
| vibration | numeric(6,2) | YES | vibration |
| current | numeric(8,2) | YES | current |
| rpm | numeric(10,2) | YES | rpm |
| pressure | numeric(8,2) | YES | pressure |

## 5.7 pump_failures
| Column | Type | Nullable | Description |
|---|---|---|---|
| failure_id | serial | NO | failure event id (PK) |
| pump_id | integer | YES | affected pump (FK) |
| failure_date | timestamp | YES | failure timestamp |
| failure_type | text | YES | failure classification |
| downtime_hours | numeric(5,2) | YES | outage duration |

## 5.8 deliveries
| Column | Type | Nullable | Description |
|---|---|---|---|
| delivery_id | serial | NO | delivery id (PK) |
| date | date | YES | delivery date |
| source | text | YES | source warehouse |
| destination | text | YES | destination station |
| product_type | text | YES | product type |
| volume_ton | numeric(10,2) | YES | volume |
| cost_usd | numeric(10,2) | YES | cost |
| delay_hours | numeric(6,2) | YES | delay |
| distance_km | numeric(8,2) | YES | distance |
| weather_conditions | text | YES | weather |
| driver_id | integer | YES | driver id (FK) |
| vehicle_id | integer | YES | vehicle id (FK) |

## 5.9 drivers
| Column | Type | Nullable | Description |
|---|---|---|---|
| driver_id | serial | NO | driver identifier (PK) |
| name | text | NO | driver name |
| experience_years | integer | YES | years of experience |
| region | text | YES | region |

## 5.10 vehicles
| Column | Type | Nullable | Description |
|---|---|---|---|
| vehicle_id | serial | NO | vehicle identifier (PK) |
| plate_number | text | YES | plate number |
| capacity_ton | numeric(8,2) | YES | capacity |
| fuel_type | text | YES | fuel type |

## 5.11 oil_stations
| Column | Type | Nullable | Description |
|---|---|---|---|
| station_id | serial | NO | station identifier (PK) |
| station_name | varchar(100) | YES | station name |
| latitude | float | YES | latitude |
| longitude | float | YES | longitude |
| oil_flow_per_day | float | YES | oil flow |

## 5.12 etl_metadata.loaded_partitions
| Column | Type | Nullable | Description |
|---|---|---|---|
| table_name | text | NO | table name (PK) |
| partition_date | date | NO | partition date (PK) |
| status | text | NO | status (processing/loaded/failed) |
| loaded_at | timestamptz | YES | loaded at |
| updated_at | timestamptz | YES | updated at |
| dag_run_id | text | YES | dag run id |

## 5.13 etl_metadata.pipeline_watermarks
| Column | Type | Nullable | Description |
|---|---|---|---|
| dataset | varchar(100) | NO | dataset name (PK) |
| last_processed_watermark | timestamp | YES | high watermark |
| updated_at | timestamp | YES | last update |

## 5.14 etl_metadata.dq_validation_results
| Column | Type | Nullable | Description |
|---|---|---|---|
| dataset | varchar(100) | NO | dataset name |
| validation_type | varchar(100) | NO | type of check |
| partition_date | date | NO | data partition |
| execution_date | date | NO | airflow ds |
| status | varchar(20) | YES | PASS/FAIL |
| failed_rows | bigint | YES | count of failures |
| checked_rows | bigint | YES | total count |
| message | text | YES | error detail |
| created_at | timestamp | YES | check start |
| updated_at | timestamp | YES | upsert time |

## 5.15 gold.mart_production
| Column | Type | Nullable | Description |
|---|---|---|---|
| mart_id | bigserial | NO | mart id (PK) |
| well_id | integer | NO | well id |
| date | date | NO | date |
| oil_ton | numeric(12,3) | YES | oil ton |
| gas_m3 | numeric(14,2) | YES | gas m3 |
| water_m3 | numeric(14,2) | YES | water m3 |
| energy_kwh | numeric(14,2) | YES | energy |
| downtime_hours | numeric(6,2) | YES | downtime |
| avg_temperature | numeric(6,2) | YES | avg temp |
| avg_pressure | numeric(8,2) | YES | avg pressure |
| avg_pump_speed_rpm | numeric(10,2) | YES | avg rpm |
| avg_oil_flow_rate | numeric(10,3) | YES | avg flow |
| max_vibration | numeric(6,2) | YES | max vibration |
| daily_target_ton | numeric(12,3) | YES | target |
| production_efficiency | numeric(8,4) | YES | efficiency |
| downtime_pct | numeric(6,3) | YES | downtime pct |
| load_timestamp | timestamp | YES | load ts |
| partition_date | date | YES | partition date (generated) |


## 5.16 gold.mart_well_kpi
| Column | Type | Nullable | Description |
|---|---|---|---|
| well_id | integer | NO | well id (PK) |
| date | date | NO | date (PK) |
| avg_daily_oil | numeric(12,3) | YES | avg oil |
| total_oil | numeric(14,3) | YES | total oil |
| avg_downtime_pct | numeric(6,3) | YES | avg downtime |
| avg_efficiency | numeric(8,4) | YES | avg efficiency |
| best_day_oil | numeric(12,3) | YES | best day |
| worst_day_oil | numeric(12,3) | YES | worst day |
| production_rank | integer | YES | rank |
| performance_group | text | YES | group |
| load_timestamp | timestamp | YES | load ts |
| partition_date | date | YES | partition date (generated) |

## 5.17 gold.mart_failures
| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | bigserial | NO | record id (PK) |
| pump_id | integer | NO | pump id |
| well_id | integer | NO | well id |
| date | date | NO | date |
| timestamp | timestamp | YES | event timestamp |
| temperature | numeric(6,2) | YES | temperature |
| vibration | numeric(6,2) | YES | vibration |
| current | numeric(8,2) | YES | current |
| rpm | numeric(10,2) | YES | rpm |
| pressure | numeric(8,2) | YES | pressure |
| vibration_zscore | numeric(6,3) | YES | vibration zscore |
| temperature_zscore | numeric(6,3) | YES | temperature zscore |
| is_anomaly | boolean | YES | is anomaly |
| anomaly_reason | text[] | YES | reason |
| failure_type | text | YES | failure type |
| is_failure | boolean | YES | is failure |
| risk_score | numeric(5,4) | YES | risk score |
| failure_probability | numeric(5,4) | YES | failure prob |
| load_timestamp | timestamp | YES | load ts |
| partition_date | date | YES | partition date (generated) |

## 5.18 gold.mart_logistics
| Column | Type | Nullable | Description |
|---|---|---|---|
| delivery_id | bigint | NO | delivery id (PK) |
| date | date | NO | date |
| source | text | YES | source warehouse |
| destination | text | YES | destination |
| product_type | text | YES | product type |
| volume_ton | numeric(12,3) | YES | volume |
| cost_usd | numeric(14,2) | YES | cost |
| delay_hours | numeric(8,2) | YES | delay |
| distance_km | numeric(10,2) | YES | distance |
| weather_conditions | text | YES | weather |
| driver_id | integer | YES | driver id |
| driver_name | text | YES | driver name |
| experience_years | integer | YES | exp |
| vehicle_id | integer | YES | vehicle id |
| plate_number | text | YES | plate |
| capacity_ton | numeric(8,2) | YES | capacity |
| fuel_type | text | YES | fuel type |
| cost_per_km | numeric(10,2) | YES | cost/km |
| cost_per_ton | numeric(10,2) | YES | cost/ton |
| delay_flag | boolean | YES | delay flag |
| weather_impact | text | YES | impact |
| load_timestamp | timestamp | YES | load ts |
| partition_date | date | YES | partition date (generated) |
