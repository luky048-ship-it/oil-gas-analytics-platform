# Schema Contracts — Oil Production Analytics Platform

## 1. Purpose

Данный документ определяет schema contracts для платформы аналитики нефтедобычи.

Цели:

* стандартизация структуры данных между source systems, ingestion layer и analytical layer;
* обеспечение совместимости пайплайнов;
* предотвращение schema drift;
* определение правил качества данных;
* фиксация business semantics для downstream ML/BI/forecasting workloads.

Контракты являются обязательными для:

* Data Producers;
* ETL/ELT pipelines;
* Streaming jobs;
* Data Quality services;
* ML Feature pipelines;
* BI semantic layer.

---

# 2. Platform Standards

## 2.1 Storage Standards

| Layer  | Format        | Partitioning   | Retention |
| ------ | ------------- | -------------- | --------- |
| Raw    | Parquet       | ingestion_date | 365 days  |
| Bronze | Parquet       | event_date     | 730 days  |
| Silver | Parquet       | month          | 5 years   |
| Gold   | Postgres      | month          | unlimited |

---

## 2.2 Naming Conventions

### Tables

* snake_case
* pluralized entities
* no abbreviations except approved industrial abbreviations

Examples:

* wells
* production
* well_telemetry
* pump_failures

### Columns

Rules:

* snake_case only
* SI units in suffixes
* timestamps in UTC
* IDs are integer surrogate keys

Examples:

* oil_ton
* gas_m3
* energy_kwh
* downtime_hours

---

## 2.3 Data Types Standards

| Logical Type        | Physical Type |
| ------------------- | ------------- |
| Identifier          | int32         |
| Numeric KPI         | float64       |
| Date                | date32        |
| Event Timestamp     | timestamp(s)  |
| Dimension Attribute | string        |

---

## 2.4 Nullability Standards

| Category             | Nullable |
| -------------------- | -------- |
| Primary Keys         | NO       |
| Foreign Keys         | NO       |
| Metrics              | YES      |
| Dimension Attributes | YES      |
| Event Time           | NO       |

---

## 2.5 Time Standards

* All timestamps stored in UTC.
* timestamp("s") precision is mandatory.
* Dates represent operational local day.
* Telemetry timestamps must be monotonic within source partition.

---

# 3. Schema Evolution Policy

## 3.1 Allowed Changes

| Change                 | Allowed | Notes                   |
| ---------------------- | ------- | ----------------------- |
| Add nullable column    | YES     | backward compatible     |
| Add partition column   | YES     | requires migration plan |
| Increase string length | YES     | safe                    |
| Add enum values        | YES     | documented              |

---

## 3.2 Forbidden Changes

| Change                      | Allowed |
| --------------------------- | ------- |
| Rename column               | NO      |
| Change datatype             | NO      |
| Remove column               | NO      |
| Re-purpose column semantics | NO      |
| Change units of measurement | NO      |

---

# 4. Data Quality Contracts

## 4.1 Data Quality Dimensions

| Dimension             | Description                  |
| --------------------- | ---------------------------- |
| Completeness          | mandatory fields populated   |
| Validity              | values inside allowed ranges |
| Uniqueness            | no duplicate PKs             |
| Referential Integrity | FK consistency               |
| Freshness             | SLA compliance               |
| Accuracy              | industrial sanity checks     |

---

## 4.2 Severity Levels

| Severity | Action                  |
| -------- | ----------------------- |
| CRITICAL | pipeline fail           |
| HIGH     | quarantine invalid rows |
| MEDIUM   | warning                 |
| LOW      | audit only              |

---

## 4.3 Global Validation Rules

### Primary Keys

* must be unique;
* must not be null.

### Foreign Keys

* must reference existing dimensions;
* orphan records are quarantined.

### Numeric Metrics

* no NaN;
* no Infinity;
* industrial range validation mandatory.

---

# 5. Table Contracts

---

# 5.1 wells

## Business Description

Master data for oil wells.

## Primary Key

* well_id

## Schema

| Column     | Type   | Nullable | Description                                |
| ---------- | ------ | -------- | ------------------------------------------ |
| well_id    | int32  | NO       | unique well identifier                     |
| name       | string | NO       | operational well name                      |
| field_name | string | NO       | oil field name                             |
| region     | string | NO       | production region                          |
| start_date | date32 | NO       | production start date                      |
| operator   | string | YES      | operating company                          |
| status     | string | NO       | active/inactive/maintenance/decommissioned |

## Constraints

| Rule                       | Severity |
| -------------------------- | -------- |
| well_id unique             | CRITICAL |
| start_date <= current_date | HIGH     |
| status in approved enum    | HIGH     |

## Approved Status Values

* active
* inactive
* maintenance
* decommissioned

## SLA

| Metric            | Value |
| ----------------- | ----- |
| Refresh Frequency | daily |
| Freshness SLA     | 24h   |

---

# 5.2 production

## Business Description

Daily production metrics per well.

## Primary Key

* prod_id

## Foreign Keys

* well_id → wells.well_id

## Schema

| Column         | Type    | Nullable | Description                    |
| -------------- | ------- | -------- | ------------------------------ |
| prod_id        | int32   | NO       | production record id           |
| well_id        | int32   | NO       | well reference                 |
| date           | date32  | NO       | production day                 |
| oil_ton        | float64 | YES      | oil production in tons         |
| gas_m3         | float64 | YES      | gas production in cubic meters |
| water_m3       | float64 | YES      | water production               |
| energy_kwh     | float64 | YES      | consumed energy                |
| downtime_hours | float64 | YES      | downtime duration              |
| temperature    | float64 | YES      | average operating temperature  |
| pressure       | float64 | YES      | average pressure               |

## Constraints

| Rule                            | Severity |
| ------------------------------- | -------- |
| prod_id unique                  | CRITICAL |
| well_id exists in wells         | CRITICAL |
| oil_ton >= 0                    | HIGH     |
| gas_m3 >= 0                     | HIGH     |
| water_m3 >= 0                   | HIGH     |
| energy_kwh >= 0                 | HIGH     |
| downtime_hours between 0 and 24 | HIGH     |
| pressure between 0 and 1000     | MEDIUM   |
| temperature between -60 and 250 | MEDIUM   |

## Partitioning

Partition by:

* date

## SLA

| Metric            | Value |
| ----------------- | ----- |
| Refresh Frequency | daily |
| Freshness SLA     | 24h    |

---

# 5.3 well_telemetry

## Business Description

High-frequency telemetry from wells.

## Primary Key

* record_id

## Foreign Keys

* well_id → wells.well_id

## Schema

| Column         | Type         | Nullable | Description           |
| -------------- | ------------ | -------- | --------------------- |
| record_id      | int32        | NO       | telemetry record id   |
| well_id        | int32        | NO       | well reference        |
| timestamp      | timestamp(s) | NO       | event timestamp UTC   |
| pump_speed_rpm | float64      | YES      | pump rotation speed   |
| pump_current   | float64      | YES      | electrical current    |
| pressure_in    | float64      | YES      | inlet pressure        |
| pressure_out   | float64      | YES      | outlet pressure       |
| temperature    | float64      | YES      | operating temperature |
| vibration      | float64      | YES      | vibration metric      |
| oil_flow_rate  | float64      | YES      | real-time flow rate   |

## Constraints

| Rule                        | Severity |
| --------------------------- | -------- |
| record_id unique            | CRITICAL |
| timestamp not null          | CRITICAL |
| well_id exists              | CRITICAL |
| pump_speed_rpm >= 0         | HIGH     |
| vibration >= 0              | HIGH     |
| oil_flow_rate >= 0          | HIGH     |
| pressure_out >= pressure_in | MEDIUM   |

## Streaming Requirements

| Requirement         | Value          |
| ------------------- | -------------- |
| Late Arrival Window | 10 minutes     |
| Deduplication Key   | record_id      |
| Ordering Key        | timestamp      |
| Expected Frequency  | every 1–10 sec |

## Partitioning

Partition by:

* event_date

Cluster by:

* well_id

---

# 5.4 well_targets

## Business Description

Daily oil production targets.

## Composite Key

* well_id
* date

## Foreign Keys

* well_id → wells.well_id

## Schema

| Column        | Type    | Nullable | Description        |
| ------------- | ------- | -------- | ------------------ |
| well_id       | int32   | NO       | well reference     |
| date          | date32  | NO       | target date        |
| daily_oil_ton | float64 | NO       | planned production |

## Constraints

| Rule                  | Severity |
| --------------------- | -------- |
| unique(well_id, date) | CRITICAL |
| daily_oil_ton >= 0    | HIGH     |

---

# 5.5 pumps

## Business Description

Pump asset registry.

## Primary Key

* pump_id

## Foreign Keys

* well_id → wells.well_id

## Schema

| Column       | Type   | Nullable | Description       |
| ------------ | ------ | -------- | ----------------- |
| pump_id      | int32  | NO       | unique pump id    |
| well_id      | int32  | NO       | linked well       |
| type         | string | NO       | pump type         |
| install_date | date32 | NO       | installation date |
| manufacturer | string | YES      | OEM manufacturer  |
| model        | string | YES      | model identifier  |

## Constraints

| Rule                         | Severity |
| ---------------------------- | -------- |
| pump_id unique               | CRITICAL |
| install_date <= current_date | HIGH     |

---

# 5.6 pump_sensors

## Business Description

Telemetry from pump sensor systems.

## Primary Key

* record_id

## Foreign Keys

* pump_id → pumps.pump_id

## Schema

| Column      | Type         | Nullable | Description        |
| ----------- | ------------ | -------- | ------------------ |
| record_id   | int32        | NO       | sensor record id   |
| pump_id     | int32        | NO       | pump reference     |
| timestamp   | timestamp(s) | NO       | event timestamp    |
| temperature | float64      | YES      | temperature metric |
| vibration   | float64      | YES      | vibration level    |
| current     | float64      | YES      | electrical current |
| rpm         | float64      | YES      | rotational speed   |
| pressure    | float64      | YES      | pressure metric    |

## Constraints

| Rule             | Severity |
| ---------------- | -------- |
| record_id unique | CRITICAL |
| vibration >= 0   | HIGH     |
| rpm >= 0         | HIGH     |
| pressure >= 0    | HIGH     |

## Streaming Requirements

| Requirement | Value     |
| ----------- | --------- |
| Frequency   | 1–5 sec   |
| Late Events | 5 min     |
| Ordering    | timestamp |

---

# 5.7 pump_failures

## Business Description

Pump failure events and downtime tracking.

## Primary Key

* failure_id

## Foreign Keys

* pump_id → pumps.pump_id

## Schema

| Column         | Type         | Nullable | Description            |
| -------------- | ------------ | -------- | ---------------------- |
| failure_id     | int32        | NO       | failure event id       |
| pump_id        | int32        | NO       | affected pump          |
| failure_date   | timestamp(s) | NO       | failure timestamp      |
| failure_type   | string       | NO       | failure classification |
| downtime_hours | float64      | YES      | outage duration        |

## Constraints

| Rule                   | Severity |
| ---------------------- | -------- |
| failure_id unique      | CRITICAL |
| downtime_hours >= 0    | HIGH     |
| failure_type not empty | HIGH     |

## Approved Failure Types

* electrical
* mechanical
* overheating
* seal_failure
* vibration_alarm
* pressure_loss
* unknown

---

# 5.8 deliveries

## Business Description

Oil logistics and transportation events.

## Primary Key

* delivery_id

## Foreign Keys

* driver_id → drivers.driver_id
* vehicle_id → vehicles.vehicle_id

## Schema

| Column             | Type    | Nullable | Description          |
| ------------------ | ------- | -------- | -------------------- |
| delivery_id        | int32   | NO       | delivery event id    |
| date               | date32  | NO       | delivery date        |
| source             | string  | NO       | origin location      |
| destination        | string  | NO       | destination location |
| product_type       | string  | NO       | transported product  |
| volume_ton         | float64 | YES      | transported volume   |
| cost_usd           | float64 | YES      | delivery cost        |
| delay_hours        | float64 | YES      | transport delay      |
| distance_km        | float64 | YES      | travel distance      |
| weather_conditions | string  | YES      | weather snapshot     |
| driver_id          | int32   | NO       | assigned driver      |
| vehicle_id         | int32   | NO       | assigned vehicle     |

## Constraints

| Rule               | Severity |
| ------------------ | -------- |
| delivery_id unique | CRITICAL |
| volume_ton >= 0    | HIGH     |
| cost_usd >= 0      | HIGH     |
| delay_hours >= 0   | MEDIUM   |
| distance_km > 0    | HIGH     |

## Approved Product Types

* crude_oil
* condensate
* diesel
* drilling_fluids

---

# 5.9 drivers

## Business Description

Driver master registry.

## Primary Key

* driver_id

## Schema

| Column           | Type   | Nullable | Description        |
| ---------------- | ------ | -------- | ------------------ |
| driver_id        | int32  | NO       | unique driver id   |
| name             | string | NO       | driver full name   |
| experience_years | int32  | YES      | driving experience |
| region           | string | YES      | operating region   |

## Constraints

| Rule                              | Severity |
| --------------------------------- | -------- |
| driver_id unique                  | CRITICAL |
| experience_years between 0 and 60 | MEDIUM   |

---

# 5.10 vehicles

## Business Description

Fleet registry.

## Primary Key

* vehicle_id

## Schema

| Column       | Type    | Nullable | Description         |
| ------------ | ------- | -------- | ------------------- |
| vehicle_id   | int32   | NO       | unique vehicle id   |
| plate_number | string  | NO       | license plate       |
| capacity_ton | float64 | YES      | maximum capacity    |
| fuel_type    | string  | YES      | fuel classification |

## Constraints

| Rule                | Severity |
| ------------------- | -------- |
| vehicle_id unique   | CRITICAL |
| plate_number unique | HIGH     |
| capacity_ton > 0    | HIGH     |

## Approved Fuel Types

* diesel
* gasoline
* electric
* hybrid
* lng

---

# 5.11 oil_stations

## Business Description

Oil station operational registry.

## Primary Key

* station_id

## Schema

| Column           | Type    | Nullable | Description          |
| ---------------- | ------- | -------- | -------------------- |
| station_id       | int32   | NO       | station identifier   |
| station_name     | string  | NO       | station name         |
| latitude         | float64 | NO       | geographic latitude  |
| longitude        | float64 | NO       | geographic longitude |
| oil_flow_per_day | float64 | YES      | daily throughput     |

## Constraints

| Rule                           | Severity |
| ------------------------------ | -------- |
| station_id unique              | CRITICAL |
| latitude between -90 and 90    | HIGH     |
| longitude between -180 and 180 | HIGH     |
| oil_flow_per_day >= 0          | HIGH     |

---

# 5.12 etl_metadata.loaded_partitions

## Business Description

Metadata table for incremental ingestion orchestration and partition tracking.

## Composite Primary Key

- table_name
- partition_date

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| table_name | text | NO | source table identifier |
| partition_date | date | NO | loaded business partition |
| status | text | NO | loading state |
| loaded_at | timestamp with timezone | YES | initial load timestamp |
| updated_at | timestamp with timezone | YES | latest metadata update |
| dag_run_id | text | YES | orchestration DAG execution id |

## Constraints

| Rule | Severity |
|---|---|
| unique(table_name, partition_date) | CRITICAL |
| status in approved enum | HIGH |
| partition_date <= current_date + 1 | HIGH |

## Approved Status Values

- processing
- loaded
- failed
- skipped

## Operational Semantics

| Attribute | Value |
|---|---|
| Update Pattern | UPSERT |
| Retention | permanent |
| Usage | orchestration checkpointing |
| Access Pattern | high-frequency lookup |

## SLA

| Metric | Value |
|---|---|
| Update Latency | < 1 min |
| Consistency | strong |

---

# 5.13 etl_metadata.marts_loaded_partitions

## Business Description

Metadata table for Gold/DataMart incremental load tracking.

## Composite Primary Key

- mart_name
- partition_date

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| mart_name | varchar(100) | NO | mart identifier |
| partition_date | date | NO | processed partition |
| dag_run_id | varchar(250) | YES | orchestrator DAG run id |
| loaded_at | timestamp | YES | mart load timestamp |

## Constraints

| Rule | Severity |
|---|---|
| unique(mart_name, partition_date) | CRITICAL |
| partition_date <= current_date + 1 | HIGH |

## Operational Semantics

| Attribute | Value |
|---|---|
| Update Pattern | INSERT ONLY |
| Usage | mart checkpoint tracking |
| Retention | permanent |

---

# 5.14 gold.mart_production

## Business Description

Gold-layer production mart aggregated by well and operational day.

## Primary Key

- mart_id

## Business Grain

1 row = 1 well × 1 operational day

## Foreign Keys

- well_id → wells.well_id

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| mart_id | bigint | NO | surrogate mart identifier |
| well_id | integer | NO | well reference |
| date | date | NO | operational date |
| oil_ton | numeric(12,3) | YES | daily oil production |
| gas_m3 | numeric(14,2) | YES | daily gas production |
| water_m3 | numeric(14,2) | YES | daily water production |
| energy_kwh | numeric(14,2) | YES | consumed energy |
| downtime_hours | numeric(6,2) | YES | downtime duration |
| avg_temperature | numeric(6,2) | YES | average telemetry temperature |
| avg_pressure | numeric(8,2) | YES | average telemetry pressure |
| avg_pump_speed_rpm | numeric(10,2) | YES | average pump RPM |
| avg_oil_flow_rate | numeric(10,3) | YES | average flow rate |
| max_vibration | numeric(6,2) | YES | max observed vibration |
| daily_target_ton | numeric(12,3) | YES | planned target |
| production_efficiency | numeric(8,4) | YES | actual vs target ratio |
| downtime_pct | numeric(6,3) | YES | operational downtime percentage |
| load_timestamp | timestamp | YES | ETL load timestamp |
| partition_date | date | YES | generated partition column |

## Constraints

| Rule | Severity |
|---|---|
| production_efficiency between 0 and 10 | MEDIUM |
| downtime_pct between 0 and 100 | HIGH |
| oil_ton >= 0 | HIGH |

## Partitioning

Partition by:

- partition_date

Cluster by:

- well_id

---

# 5.15 gold.mart_well_kpi

## Business Description

Analytical KPI mart for well performance benchmarking.

## Composite Primary Key

- well_id
- date

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| well_id | integer | NO | well identifier |
| date | date | NO | KPI calculation date |
| avg_daily_oil | numeric(12,3) | YES | rolling average oil |
| total_oil | numeric(14,3) | YES | cumulative oil |
| avg_downtime_pct | numeric(6,3) | YES | average downtime |
| avg_efficiency | numeric(8,4) | YES | average production efficiency |
| best_day_oil | numeric(12,3) | YES | max daily production |
| worst_day_oil | numeric(12,3) | YES | min daily production |
| production_rank | integer | YES | rank by production |
| performance_group | text | YES | performance classification |
| load_timestamp | timestamp | YES | ETL timestamp |
| partition_date | date | YES | generated partition |

## Approved Performance Groups

- Top
- Good
- Average
- Poor

---

# 5.16 gold.mart_failures

## Business Description

Predictive maintenance and anomaly detection mart.

## Primary Key

- record_id

## Foreign Keys

- pump_id → pumps.pump_id
- well_id → wells.well_id

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | bigint | NO | mart record id |
| pump_id | integer | NO | pump reference |
| well_id | integer | NO | well reference |
| date | date | NO | operational date |
| timestamp | timestamp | YES | event timestamp |
| temperature | numeric(6,2) | YES | sensor temperature |
| vibration | numeric(6,2) | YES | vibration metric |
| current | numeric(8,2) | YES | electrical current |
| rpm | numeric(10,2) | YES | rotation speed |
| pressure | numeric(8,2) | YES | pressure metric |
| vibration_zscore | numeric(6,3) | YES | anomaly z-score |
| temperature_zscore | numeric(6,3) | YES | anomaly z-score |
| is_anomaly | boolean | YES | anomaly indicator |
| anomaly_reason | text[] | YES | anomaly explanations |
| failure_type | text | YES | classified failure |
| is_failure | boolean | YES | failure occurrence flag |
| risk_score | numeric(5,4) | YES | predictive risk score |
| failure_probability | numeric(5,4) | YES | ML failure probability |
| load_timestamp | timestamp | YES | ETL load timestamp |
| partition_date | date | YES | generated partition |

## Constraints

| Rule | Severity |
|---|---|
| risk_score between 0 and 1 | HIGH |
| failure_probability between 0 and 1 | HIGH |

---

# 5.17 gold.mart_logistics

## Business Description

Gold-layer logistics and transportation analytics mart.

## Primary Key

- delivery_id

## Foreign Keys

- driver_id → drivers.driver_id
- vehicle_id → vehicles.vehicle_id

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| delivery_id | bigint | NO | delivery identifier |
| date | date | NO | delivery date |
| source | text | YES | source location |
| destination | text | YES | destination location |
| product_type | text | YES | transported product |
| volume_ton | numeric(12,3) | YES | transported volume |
| cost_usd | numeric(14,2) | YES | transportation cost |
| delay_hours | numeric(8,2) | YES | transport delay |
| distance_km | numeric(10,2) | YES | route distance |
| weather_conditions | text | YES | weather snapshot |
| driver_id | integer | YES | assigned driver |
| driver_name | text | YES | driver full name |
| experience_years | integer | YES | driver experience |
| vehicle_id | integer | YES | vehicle reference |
| plate_number | text | YES | vehicle plate |
| capacity_ton | numeric(8,2) | YES | vehicle capacity |
| fuel_type | text | YES | vehicle fuel |
| cost_per_km | numeric(10,2) | YES | derived logistics KPI |
| cost_per_ton | numeric(10,2) | YES | derived cost KPI |
| delay_flag | boolean | YES | SLA breach indicator |
| weather_impact | text | YES | weather impact category |
| load_timestamp | timestamp | YES | ETL load timestamp |
| partition_date | date | YES | generated partition |

## Approved Weather Impact Values

- high
- medium
- low
---

# 6. Referential Integrity Matrix

| Child Table    | FK Column  | Parent Table | Parent Column |
| -------------- | ---------- | ------------ | ------------- |
| production     | well_id    | wells        | well_id       |
| well_telemetry | well_id    | wells        | well_id       |
| well_targets   | well_id    | wells        | well_id       |
| pumps          | well_id    | wells        | well_id       |
| pump_sensors   | pump_id    | pumps        | pump_id       |
| pump_failures  | pump_id    | pumps        | pump_id       |
| deliveries     | driver_id  | drivers      | driver_id     |
| deliveries     | vehicle_id | vehicles     | vehicle_id    |
| gold.mart_production | well_id | wells | well_id |
| gold.mart_well_kpi | well_id | wells | well_id |
| gold.mart_failures | pump_id | pumps | pump_id |
| gold.mart_failures | well_id | wells | well_id |
| gold.mart_logistics | driver_id | drivers | driver_id |
| gold.mart_logistics | vehicle_id | vehicles | vehicle_id |

---

# 7. Observability & Monitoring

## 7.1 Pipeline Metrics

| Metric             | Description               |
| ------------------ | ------------------------- |
| ingestion_lag      | source to bronze delay    |
| processing_latency | transformation duration   |
| dq_failure_rate    | invalid records ratio     |
| duplicate_rate     | duplicate primary keys    |
| null_rate          | mandatory null percentage |

---

## 7.2 Alert Thresholds

| Condition             | Threshold | Severity |
| --------------------- | --------- | -------- |
| Freshness SLA breach  | > 30 min  | HIGH     |
| DQ failures           | > 5%      | HIGH     |
| Duplicate keys        | > 0.1%    | MEDIUM   |
| Null mandatory fields | > 0       | CRITICAL |

---

# 8. Security & Governance

## 8.1 Classification

| Dataset     | Classification |
| ----------- | -------------- |
| telemetry   | confidential   |
| production  | confidential   |
| logistics   | internal       |
| master_data | internal       |

---

## 8.2 Access Control

| Role            | Access     |
| --------------- | ---------- |
| Data Engineer   | read/write |
| Data Scientist  | read       |
| BI Analyst      | gold only  |
| External Vendor | restricted |

---

# 9. Contract Testing

* schema validation;
* datatype validation;
* nullability validation;
* uniqueness validation;
* referential integrity validation;
* range validation;
* freshness validation.

---

# 10. Recommended Partitioning Strategy

| Table          | Partitioning  | Clustering |
| -------------- | ------------- | ---------- |
| production     | date          | well_id    |
| well_telemetry | event_date    | well_id    |
| pump_sensors   | event_date    | pump_id    |
| deliveries     | date          | source     |
| pump_failures  | failure_month | pump_id    |
| etl_metadata.loaded_partitions | partition_date | table_name |
| etl_metadata.marts_loaded_partitions | partition_date | mart_name |
| gold.mart_production | partition_date | well_id |
| gold.mart_well_kpi | partition_date | performance_group |
| gold.mart_failures | partition_date | pump_id |
| gold.mart_logistics | partition_date | driver_id |

