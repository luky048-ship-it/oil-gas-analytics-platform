# Описание таблиц, создаваемых при инициализации базы данных

## wells
Справочная информация о скважинах.

| Столбец      | Тип            | Описание                     |
|--------------|----------------|------------------------------|
| well_id      | SERIAL PK      | Уникальный идентификатор     |
| name         | TEXT NOT NULL  | Наименование скважины         |
| field_name   | TEXT           | Название месторождения        |
| region       | TEXT           | Регион добычи                |
| start_date   | DATE           | Дата начала эксплуатации      |
| operator     | TEXT           | Оператор (компания)           |
| status       | TEXT           | Статус — active / suspended / maintenance |

## production
Ежедневные производственные показатели.

| Столбец          | Тип                | Описание                              |
|------------------|--------------------|---------------------------------------|
| prod_id          | SERIAL PK          | Уникальный идентификатор записи       |
| well_id          | INT → wells.well_id| Ссылка на скважину                    |
| date             | DATE NOT NULL      | Дата измерения                        |
| oil_ton          | NUMERIC(10,2)      | Добыто нефти, тонн                    |
| gas_m3           | NUMERIC(12,2)      | Добыто газа, м³                       |
| water_m3         | NUMERIC(12,2)      | Добыто воды, м³                       |
| energy_kwh       | NUMERIC(12,2)      | Потребление энергии, кВт·ч            |
| downtime_hours   | NUMERIC(5,2)       | Часы простоя                          |
| temperature      | NUMERIC(5,2)       | Температура, °C                       |
| pressure         | NUMERIC(5,2)       | Давление, бар                         |

## well_telemetry
Почасовые параметры работы оборудования.

| Столбец          | Тип                | Описание                              |
|------------------|--------------------|---------------------------------------|
| record_id        | SERIAL PK          | Уникальный идентификатор записи       |
| well_id          | INT → wells.well_id| Ссылка на скважину                    |
| timestamp        | TIMESTAMP          | Временная метка                        |
| pump_speed_rpm   | NUMERIC(8,2)       | Обороты насоса, об/мин                |
| pump_current     | NUMERIC(8,2)       | Ток насоса, А                         |
| pressure_in      | NUMERIC(8,2)       | Входное давление, бар                 |
| pressure_out     | NUMERIC(8,2)       | Выходное давление, бар                |
| temperature      | NUMERIC(5,2)       | Температура, °C                       |
| vibration        | NUMERIC(5,2)       | Вибрация, м/с²                        |
| oil_flow_rate    | NUMERIC(8,2)       | Текущий дебит нефти, т/ч              |

## well_targets
Целевые значения для модели (дневной объём нефти).

| Столбец      | Тип                | Описание                     |
|--------------|--------------------|------------------------------|
| well_id      | INT → wells.well_id| Ссылка на скважину            |
| date         | DATE               | Дата                         |
| daily_oil_ton| NUMERIC(10,2)      | Плановый объём нефти, тонн   |

## pumps
Справочник насосов.

| Столбец       | Тип                 | Описание                     |
|---------------|---------------------|------------------------------|
| pump_id       | SERIAL PK           | Уникальный идентификатор     |
| well_id       | INT → wells.well_id | Ссылка на скважину            |
| type          | TEXT                | Тип насоса                    |
| install_date  | DATE                | Дата установки                |
| manufacturer  | TEXT                | Производитель                |
| model         | TEXT                | Модель                        |

## pump_sensors
Потоковые данные о состоянии насосов.

| Столбец     | Тип                 | Описание                     |
|-------------|---------------------|------------------------------|
| record_id   | SERIAL PK           | Уникальный идентификатор     |
| pump_id     | INT → pumps.pump_id | Ссылка на насос               |
| timestamp   | TIMESTAMP           | Временная метка               |
| temperature | NUMERIC(5,2)        | Температура, °C              |
| vibration   | NUMERIC(5,2)        | Вибрация, м/с²               |
| current     | NUMERIC(8,2)        | Ток, А                       |
| rpm         | NUMERIC(8,2)        | Обороты в минуту             |
| pressure    | NUMERIC(8,2)        | Давление, бар                |

## pump_failures
Факты отказов насосов.

| Столбец        | Тип                 | Описание                              |
|----------------|---------------------|---------------------------------------|
| failure_id     | SERIAL PK           | Уникальный идентификатор              |
| pump_id        | INT → pumps.pump_id | Ссылка на насос                       |
| failure_date   | TIMESTAMP           | Дата и время отказа                   |
| failure_type   | TEXT                | Тип отказа (механический, перегрев...) |
| downtime_hours | NUMERIC(5,2)        | Часы простоя                          |

## deliveries
Таблица маршрутов (склад → клиент).

| Столбец            | Тип           | Описание                             |
|--------------------|---------------|--------------------------------------|
| delivery_id        | SERIAL PK     | Уникальный идентификатор доставки     |
| date               | DATE          | Дата                                 |
| source             | TEXT          | Источник (откуда)                    |
| destination        | TEXT          | Пункт назначения                     |
| product_type       | TEXT          | Тип продукта (дизель, бензин...)     |
| volume_ton         | NUMERIC(10,2) | Объем в тоннах                       |
| cost_usd           | NUMERIC(10,2) | Стоимость в USD                      |
| delay_hours        | NUMERIC(6,2)  | Задержка в часах                     |
| distance_km        | NUMERIC(8,2)  | Расстояние в км                      |
| weather_conditions | TEXT          | Погодные условия                     |
| driver_id          | INT           | ID водителя                          |
| vehicle_id         | INT           | ID транспортного средства            |

## drivers
Таблица водителей.

| Столбец          | Тип       | Описание                     |
|------------------|-----------|------------------------------|
| driver_id        | SERIAL PK | Уникальный идентификатор     |
| name             | TEXT      | ФИО водителя                 |
| experience_years | INT       | Стаж работы (лет)            |
| region           | TEXT      | Регион                       |

## vehicles
Таблица транспорта.

| Столбец      | Тип          | Описание                         |
|--------------|--------------|----------------------------------|
| vehicle_id   | SERIAL PK    | Уникальный идентификатор         |
| plate_number | TEXT         | Номерной знак                    |
| capacity_ton | NUMERIC(8,2) | Грузоподъемность (тонн)          |
| fuel_type    | TEXT         | Тип топлива                      |

## oil_stations
Таблица нефтеперекачивающих станций.

| Столбец            | Тип          | Описание                         |
|--------------------|--------------|----------------------------------|
| station_id         | SERIAL PK    | Уникальный идентификатор         |
| station_name       | VARCHAR(100) | Название станции                 |
| latitude           | FLOAT        | Широта                           |
| longitude          | FLOAT        | Долгота                          |
| oil_flow_per_day   | FLOAT        | Поток нефти в день (тонн/барр)   |

## etl_metadata.loaded_partitions
Служебная таблица для отслеживания инкрементальной загрузки данных в сырые таблицы.

| Столбец        | Тип                       | Описание                               |
|----------------|---------------------------|----------------------------------------|
| table_name     | TEXT NOT NULL PK          | Имя загружаемой таблицы                 |
| partition_date | DATE NOT NULL PK          | Дата партиции                          |
| status         | TEXT NOT NULL DEFAULT 'processing' | Статус загрузки              |
| loaded_at      | TIMESTAMP WITH TIME ZONE DEFAULT NOW() | Время начала загрузки     |
| updated_at     | TIMESTAMP WITH TIME ZONE DEFAULT NOW() | Время последнего обновления |
| dag_run_id     | TEXT                      | ID запуска DAG                         |

## etl_metadata.marts_loaded_partitions
Служебная таблица для отслеживания загрузки витрин данных.

| Столбец        | Тип                              | Описание                     |
|----------------|----------------------------------|------------------------------|
| mart_name      | VARCHAR(100) NOT NULL PK         | Имя витрины                   |
| partition_date | DATE NOT NULL PK                 | Дата партиции                 |
| dag_run_id     | VARCHAR(250)                     | ID запуска DAG                |
| loaded_at      | TIMESTAMP DEFAULT CURRENT_TIMESTAMP | Время загрузки              |


### gold.mart_production
Витрина основных производственных показателей (день × скважина).

| Столбец                  | Тип              | Описание                                      |
|--------------------------|------------------|-----------------------------------------------|
| mart_id                  | BIGSERIAL PK     | Идентификатор записи                          |
| well_id                  | INTEGER NOT NULL | ID скважины                                   |
| date                     | DATE NOT NULL    | Дата измерения                                |
| oil_ton                  | NUMERIC(12,3)    | Добыто нефти, тонн                            |
| gas_m3                   | NUMERIC(14,2)    | Добыто газа, м³                               |
| water_m3                 | NUMERIC(14,2)    | Добыто воды, м³                               |
| energy_kwh               | NUMERIC(14,2)    | Потребление энергии, кВт·ч                    |
| downtime_hours           | NUMERIC(6,2)     | Часы простоя                                  |
| avg_temperature          | NUMERIC(6,2)     | Средняя температура за сутки, °C              |
| avg_pressure             | NUMERIC(8,2)     | Среднее давление за сутки, бар                |
| avg_pump_speed_rpm       | NUMERIC(10,2)    | Средние обороты насоса за сутки, об/мин       |
| avg_oil_flow_rate        | NUMERIC(10,3)    | Средний дебит нефти, т/ч                      |
| max_vibration            | NUMERIC(6,2)     | Максимальная вибрация за сутки, м/с²          |
| daily_target_ton         | NUMERIC(12,3)    | Плановый (целевой) объём нефти, тонн         |
| production_efficiency    | NUMERIC(8,4)     | Эффективность производства (oil_ton / target) |
| downtime_pct             | NUMERIC(6,3)     | Процент простоя                               |
| load_timestamp           | TIMESTAMP        | Время загрузки витрины                        |
| partition_date           | DATE             | Дата партиции (генерируется)                  |


### gold.mart_failures
Витрина данных по отказам, аномалиям и предиктивному обслуживанию.

| Столбец                | Тип              | Описание                                      |
|------------------------|------------------|-----------------------------------------------|
| record_id              | BIGSERIAL PK     | Идентификатор записи                          |
| pump_id                | INTEGER NOT NULL | ID насоса                                     |
| well_id                | INTEGER NOT NULL | ID скважины                                   |
| date                   | DATE NOT NULL    | Дата                                          |
| timestamp              | TIMESTAMP        | Временная метка события                       |
| temperature            | NUMERIC(6,2)     | Температура, °C                               |
| vibration              | NUMERIC(6,2)     | Вибрация, м/с²                                |
| current                | NUMERIC(8,2)     | Ток, А                                        |
| rpm                    | NUMERIC(10,2)    | Обороты, об/мин                               |
| pressure               | NUMERIC(8,2)     | Давление, бар                                 |
| vibration_zscore       | NUMERIC(6,3)     | Z-score вибрации                              |
| temperature_zscore     | NUMERIC(6,3)     | Z-score температуры                           |
| is_anomaly             | BOOLEAN          | Признак аномалии                              |
| anomaly_reason         | TEXT[]           | Массив причин аномалии                        |
| failure_type           | TEXT             | Тип отказа                                    |
| is_failure             | BOOLEAN          | Признак фактического отказа                   |
| risk_score             | NUMERIC(5,4)     | Оценка риска (0–1)                            |
| failure_probability    | NUMERIC(5,4)     | Вероятность отказа                            |
| load_timestamp         | TIMESTAMP        | Время загрузки                                |
| partition_date         | DATE             | Дата партиции                                 |


### gold.mart_logistics
Витрина логистики и доставок (обогащённая справочниками).

| Столбец            | Тип              | Описание                                      |
|--------------------|------------------|-----------------------------------------------|
| delivery_id        | BIGINT PK        | Идентификатор доставки                        |
| date               | DATE NOT NULL    | Дата доставки                                 |
| source             | TEXT             | Источник (склад)                              |
| destination        | TEXT             | Пункт назначения                              |
| product_type       | TEXT             | Тип продукта                                  |
| volume_ton         | NUMERIC(12,3)    | Объём, тонн                                   |
| cost_usd           | NUMERIC(14,2)    | Стоимость доставки, USD                       |
| delay_hours        | NUMERIC(8,2)     | Задержка, часы                                |
| distance_km        | NUMERIC(10,2)    | Расстояние, км                                |
| weather_conditions | TEXT             | Погодные условия                              |
| driver_id          | INTEGER          | ID водителя                                   |
| driver_name        | TEXT             | ФИО водителя                                  |
| experience_years   | INTEGER          | Стаж работы, лет                              |
| vehicle_id         | INTEGER          | ID транспортного средства                     |
| plate_number       | TEXT             | Номерной знак                                 |
| capacity_ton       | NUMERIC(8,2)     | Грузоподъёмность, тонн                        |
| fuel_type          | TEXT             | Тип топлива ТС                                |
| cost_per_km        | NUMERIC(10,2)    | Стоимость за км, USD                          |
| cost_per_ton       | NUMERIC(10,2)    | Стоимость за тонну, USD                       |
| delay_flag         | BOOLEAN          | Признак задержки                              |
| weather_impact     | TEXT             | Влияние погоды (high/medium/low)              |
| load_timestamp     | TIMESTAMP        | Время загрузки                                |
| partition_date     | DATE             | Дата партиции                                 |


### gold.mart_well_kpi
KPI по скважинам за дату.

| Столбец             | Тип              | Описание                                      |
|---------------------|------------------|-----------------------------------------------|
| well_id             | INTEGER NOT NULL | ID скважины                                   |
| date                | DATE NOT NULL    | Дата                                          |
| avg_daily_oil       | NUMERIC(12,3)    | Средний дневной объём нефти                   |
| total_oil           | NUMERIC(14,3)    | Суммарный объём нефти                         |
| avg_downtime_pct    | NUMERIC(6,3)     | Средний процент простоя                       |
| avg_efficiency      | NUMERIC(8,4)     | Средняя эффективность                         |
| best_day_oil        | NUMERIC(12,3)    | Лучший день по добыче                         |
| worst_day_oil       | NUMERIC(12,3)    | Худший день по добыче                         |
| production_rank     | INTEGER          | Ранг скважины по добыче                      |
| performance_group   | TEXT             | Группа производительности (Top/Good/Average/Poor) |
| load_timestamp      | TIMESTAMP        | Время загрузки                                |
| partition_date      | DATE             | Дата партиции                                 |
