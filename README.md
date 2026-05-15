# Oil & Gas Analytics Platform

Платформа аналитики нефтедобычи на базе **Medallion Architecture**.

---

## 1. Архитектура

### Общая архитектура (Medallion)

```mermaid
graph TD
    %% Стилевые настройки для темной темы и высокой четкости
    classDef orchestrator fill:#0f172a,stroke:#38bdf8,stroke-width:2px,color:#fff
    classDef storageS3 fill:#0c4a6e,stroke:#0ea5e9,stroke-width:2px,color:#fff
    classDef storageDB fill:#1e1b4b,stroke:#6366f1,stroke-width:2px,color:#fff
    classDef logic fill:#334155,stroke:#94a3b8,stroke-width:1px,color:#fff
    classDef dq fill:#450a0a,stroke:#ef4444,stroke-width:2px,color:#fff
    classDef consumer fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#fff

    %% Внешний контейнер всей системы
    subgraph Docker_Host [🐳 DOCKER COMPOSE — ENTERPRISE ANALYTICS PLATFORM]
        
        AF[Airflow 2.8 <br/> Оркестратор]:::orchestrator

        %% Источник данных
        subgraph Source_System [🗄️ SOURCE: PostgreSQL]
            Public[(public schema <br/> Операционные данные)]:::storageDB
            MetaDB[(etl_metadata <br/> Watermarks & Logs)]:::storageDB
        end

        %% Data Lake на MinIO
        subgraph Data_Lake [❄️ DATA LAKE: MinIO / S3]
            Bronze[bronze /raw/ <br/> Immutable Parquet]:::storageS3
            Silver[silver /curated/ <br/> Validated Parquet]:::storageS3
            Quarantine[quarantine <br/> Аномальные записи]:::dq
        end

        %% DWH на PostgreSQL
        subgraph DWH_Gold [🏛️ DWH: PostgreSQL]
            Staging[staging <br/> Временные буферы]:::storageDB
            Gold[(gold schema <br/> 4 Business Marts)]:::storageDB
        end

        %% Аналитический слой
        subgraph Analytics_Layer [📊 ANALYTICS & ML]
            Superset[Apache Superset <br/> BI & Dashboards]:::consumer
            Jupyter[Jupyter Notebook <br/> ML & Research]:::consumer
        end
    end

    %% Потоки данных (Data Flow)
    
    %% 1. Ingestion
    Public -- "DAG: postgres_to_minio <br/> (Extract & Load)" --> Bronze
    AF -.-> |Управление| Public
    
    %% 2. Validation & Normalization
    Bronze -- "DAG: bronze_to_silver <br/> (Validation / Deduplication)" --> Silver
    Silver -.-> |DQ Filters| Quarantine
    AF -.-> |Управление| Bronze
    
    %% 3. Mart Building
    Silver -- "DAG: silver_to_gold <br/> (Aggregation / Joins)" --> Staging
    Staging -- "Atomic Swap <br/> (Transaction)" --> Gold
    AF -.-> |Управление| Silver

    %% 4. Consumption
    Gold --> Superset
    Gold --> Jupyter
    Silver -.-> |Ad-hoc| Jupyter
    
    %% Метаданные
    AF <--> MetaDB
    MetaDB -.-> |Tracking| Bronze
    MetaDB -.-> |Tracking| Silver
    MetaDB -.-> |Tracking| Gold

    %% Легенда стилей
    class AF orchestrator
    class Bronze,Silver storageS3
    class Public,MetaDB,Staging,Gold storageDB
    class Superset,Jupyter consumer
    class Quarantine dq

```

---

# 2. Технологический стек (Technical Stack)

Платформа построена на современном стеке с акцентом на высокую производительность (**Polars**), инкрементальную обработку и соблюдение методологии **Medallion Architecture**.

### 2.1. Основные компоненты

| Слой / Область | Технология | Версия | Роль в проекте |
| :--- | :--- | :--- | :--- |
| **Orchestration** | ![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=Apache-Airflow&logoColor=white) | `2.8.1` | Оркестрация DAG, управление зависимостями и ретраями. |
| **Data Lake (S3)** | ![MinIO](https://img.shields.io/badge/MinIO-С00000?style=flat&logo=MinIO&logoColor=white) | `Latest` | Хранение слоев Bronze (Raw) и Silver (Curated) в формате Parquet. |
| **Data Processing** | ![Polars](https://img.shields.io/badge/Polars-CD792C?style=flat) ![PyArrow](https://img.shields.io/badge/PyArrow-D55E5D?style=flat&logo=Apache-Arrow&logoColor=white) | — | Быстрая обработка через Lazy Execution. Оптимизация памяти. |
| **Database (DWH)** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=PostgreSQL&logoColor=white) | `15.0` | Serving-слой (Gold), хранение метаданных и операционных данных. |
| **BI & Analytics** | ![Superset](https://img.shields.io/badge/Apache_Superset-00A699?style=flat&logo=Apache-Superset&logoColor=white) | `3.1.0` | Визуализация бизнес-метрик, построение дашбордов и SQL Lab. |
| **ML & Research** | ![Jupyter](https://img.shields.io/badge/Jupyter_Notebook-F37626?style=flat&logo=Jupyter&logoColor=white) | — | Прототипирование моделей и ad-hoc анализ данных Silver-слоя. |

### 2.2. Окружение и разработка

*   **Язык программирования:** ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=Python&logoColor=white) `3.11` (строгая типизация, dataclasses).
*   **Инфраструктура:** ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=Docker&logoColor=white) Docker Compose (контейнеризация всех сервисов).
*   **Метаданные:** PostgreSQL (схема `etl_metadata`), управление состояниями через **Watermarks**.
*   **Формат данных:** ![Apache Parquet](https://img.shields.io/badge/Apache_Parquet-000000?style=flat) — основной формат хранения для обеспечения сжатия и высокой скорости чтения (Columnar Storage).

---

### Почему выбран этот стек:
1.  **Polars вместо Pandas:** Обеспечивает многократное преимущество в скорости и параллельной обработке телеметрии без полной загрузки данных в RAM.
2.  **Medallion на S3:** Разделение Bronze/Silver на MinIO позволяет бесконечно масштабировать хранилище независимо от вычислительных мощностей.
3.  **Atomic Swap в Postgres:** Использование PostgreSQL для Gold-слоя гарантирует ACID-совместимость и мгновенный отклик дашбордов Superset.
4.  **Airflow 2.8:** Использование современных декораторов `@task` упрощает код и делает логику пайплайнов более прозрачной.

Дополнительно:

* s3fs + pyarrow для работы с MinIO
* adbc-driver-postgresql для быстрой загрузки в Gold

---

# 3. Бизнес-логика и аналитические цели

Платформа спроектирована как инструмент поддержки принятия решений (DSS) и охватывает четыре ключевых бизнес-домена. Бизнес-логика реализуется на уровнях Silver (очистка) и Gold (агрегация).

### 3.1. Домен: Аналитика добычи (Production Excellence)
**Цель:** Максимизация операционной эффективности скважинного фонда.
*   **Бизнес-индикаторы (KPI):** 
    *   *Среднесуточный дебит:* расчет фактического объема добычи в тоннах.
    *   *Коэффициент эксплуатации (Uptime):* отношение времени работы к общему времени (выявление % простоя).
*   **Логика анализа:** Сравнение скважин-лидеров и аутсайдеров для тиражирования лучших практик. Установление корреляции между физическими параметрами (пластовое давление, температура) и объемом добычи.

### 3.2. Домен: Прогнозное моделирование (ML Yield Forecasting)
**Цель:** Переход от реактивного планирования к проактивному.
*   **ML-логика:** Использование исторической телеметрии (давление, мощность насосов, температура) для предсказания дебита на следующие сутки.
*   **Бизнес-ценность:** Позволяет финансовому департаменту точнее прогнозировать выручку, а производственному — планировать нагрузку на систему сбора нефти.

### 3.3. Домен: Надежность и Предиктивное обслуживание (Reliability & Maintenance)
**Цель:** Снижение затрат на ремонт и минимизация внеплановых остановок.
*   **Логика выявления аномалий:** Применение статистических методов (Z-score) и алгоритмов Isolation Forest к потокам данных вибрации и тока для обнаружения «предвестников» поломки.
*   **Risk Scoring:** Назначение каждой единице оборудования индекса риска (от 0 до 1). При превышении порога система сигнализирует о необходимости техобслуживания.

### 3.4. Домен: Логистическая оптимизация (Supply Chain Optimization)
**Цель:** Снижение удельной стоимости транспортировки единицы продукции.
*   **Бизнес-метрики:** 
    *   *Cost per km/ton:* мониторинг эффективности затрат на логистику.
    *   *Driver Performance Index:* оценка качества работы водителей на основе соблюдения графиков.
*   **Анализ факторов:** Оценка влияния внешних условий (погода, дорожная ситуация) на задержки поставок для оптимизации маршрутной сети.

---

# 4. Архитектура Базы Данных DWH(company_data) || datalike(row)

```text

company_data
├── SCHEMA: public                  ← Операционные исходные данные
│   ├── wells
│   ├── production
│   ├── well_telemetry
│   ├── well_targets
│   ├── pumps
│   ├── pump_sensors
│   ├── pump_failures
│   ├── deliveries
│   ├── drivers
│   ├── vehicles
│   └── oil_stations
│
├── SCHEMA: etl_metadata            ← Метаданные и управление
│   ├── loaded_partitions
│   ├── pipeline_watermarks
│   ├── dq_validation_results
│   ├── dq_quarantine_registry
│   └── marts_loaded_partitions
│
├── SCHEMA: staging                 ← Временные таблицы (atomic load)
│   ├── stg_mart_production
│   ├── stg_mart_well_kpi
│   ├── stg_mart_failures
│   └── stg_mart_logistics
│
└── SCHEMA: gold                    ← Аналитические витрины
    ├── mart_production
    ├── mart_well_kpi
    ├── mart_failures
    └── mart_logistics


```

Структура Data Lake (MinIO)

```text

s3://datalake/
├── raw/           ← Bronze (неизменяемые данные из Postgres)
├── silver/        ← Silver (очищенные, нормализованные, дедуплицированные)
├── quarantine/    ← Quarantine (проблемные записи)

```

---

# 5. DAG (Airflow)

# Архитектура загрузки данных в datalike

```mermaid

graph TD
    Start((Начало задачи)) --> LockCheck{Это факт-таблица?}
    
    %% Блок блокировок
    LockCheck -- Да --> AcquireLock[1. Блокировка в Postgres <br/> loaded_partitions: 'processing']
    LockCheck -- Нет --> InitS3[2. Инициализация S3FS]
    AcquireLock --> InitS3
    
    %% Подготовка
    InitS3 --> Invalidate[3. Инвалидация старых маркеров <br/> Удаление _SUCCESS и manifest.json]
    Invalidate --> QueryDefine[4. Генерация SQL запроса <br/> С учетом фильтра по дате и ORDER BY]
    
    %% Стриминг данных
    subgraph Data_Streaming [Потоковая обработка и Трансформация]
        QueryDefine --> PG_Cursor[5. Открытие Серверного Курсора <br/> itersize = 100,000]
        PG_Cursor --> FetchChunk[6. Чтение чанка строк]
        FetchChunk --> ArrowCast[7. Приведение к схеме <br/> Cast к EXPECTED_SCHEMAS]
        ArrowCast --> S3Write[8. Запись в Parquet на MinIO <br/> Snappy Compression]
        S3Write --> FetchChunk
    end
    
    %% Финализация файлов
    FetchChunk -- Данные закончились --> IntegrityCheck{9. Проверка целостности <br/> Кол-во строк совпадает?}
    IntegrityCheck -- Ошибка --> Rollback[Удалить файл + Снять блокировку]
    IntegrityCheck -- OK --> OrphanCleanup[10. Cleanup: Удаление старых <br/> .parquet файлов в папке]
    
    %% Метаданные и коммит
    OrphanCleanup --> WriteManifest[11. Запись manifest.json <br/> Hash схемы, кол-во строк, размер]
    WriteManifest --> WriteSuccess[12. Запись _SUCCESS <br/> Сигнал готовности для Silver]
    
    %% Завершение
    WriteSuccess --> ReleaseLock[13. Снятие блокировки <br/> статус: 'loaded']
    ReleaseLock --> Lineage[14. Отправка Lineage Event <br/> OpenLineage-ready JSON]
    Lineage --> Finish((Финиш))

    %% Обработка исключений
    Rollback --> Error((Fail Task))

```

# Архитектура транспозиции данных с datalike -> silver layer

```mermaid
graph TD
    Start((Начало)) --> GetWatermark["1. Получить Watermark из Postgres\n(last_processed_at)"]
    GetWatermark --> DiscoverPartitions["2. Discovery: Поиск папок в S3\n(partition_date > watermark)"]
    
    DiscoverPartitions --> LoadBronze["3. pl.scan_parquet\n(Lazy Scan + Predicate Pushdown)"]
    
    LoadBronze --> SchemaValidation{"4. Валидация схемы"}
    SchemaValidation -- Mismatch --> Fail["AirflowFailException\n(Schema Drift)"]
    
    SchemaValidation -- Match --> Normalization["5. Нормализация\n(UTC, типы, обрезка строк)"]
    
    Normalization --> Deduplication["6. Дедупликация\n(Unique по Business Key + TS)"]
    
    Deduplication --> MissingHandler["7. Обработка пропусков\n(Forward Fill / Interpolation)"]
    
    MissingHandler --> OutlierDetection{"8. Расчет выбросов\n(IQR / Z-score)"}
    
    %% Разветвление (Fork)
    OutlierDetection -- "is_outlier == True" --> WriteQuarantine["9. Запись в КАРАНТИН\n(S3: служебные колонки + аномалии)"]
    
    OutlierDetection -- "is_outlier == False" --> Enrichment["10. Обогащение (Join)\n(Справочники: Скважины, Насосы)"]
    
    Enrichment --> Aggregation["11. Агрегация\n(Daily Rollup по event_time)"]
    
    Aggregation --> WriteSilver["12. Атомарная запись в SILVER\n(S3: pyarrow.dataset.write_dataset)"]
    
    WriteSilver --> UpdateWatermark["13. Обновить Watermark в Postgres\n(max event_time)"]
    
    UpdateWatermark --> PublishMeta["14. Публикация метаданных\n(Metrics, Row Counts, Duration)"]
    
    WriteQuarantine --> PublishMeta
    PublishMeta --> Finish((Финиш))
```


# Архитектура транспозиции данных с silver layer -> gold layer
```mermaid
graph TD
    %% Инициализация
    Start((Начало)) --> GetWatermark[1. Чтение Watermark и метаданных из Postgres]
    GetWatermark --> DiscoverPartitions[2. Поиск новых партиций в S3 /silver/]
    
    %% Загрузка (Polars Lazy)
    DiscoverPartitions --> LoadSilver[3. Сканирование Silver: добыча, телеметрия, датчики, отказы, логистика, НСИ]
    
    %% Валидация
    LoadSilver --> ValidateSilver[4. Проверка бизнес-контрактов и схем]

    %% Блок трансформаций (Polars Lazy)
    subgraph Transformation_Block [Генерация Витрин - Бизнес-логика]
        ValidateSilver --> M1[Витрина: ДОБЫЧА <br/> mart_production]
        ValidateSilver --> M3[Витрина: ОТКАЗЫ И РИСКИ <br/> mart_failures]
        ValidateSilver --> M4[Витрина: ЛОГИСТИКА <br/> mart_logistics]
        
        %% Зависимая витрина (Строится на основе Добычи)
        M1 --> M2[Витрина: KPI СКВАЖИН <br/> mart_well_kpi]
    end

    %% Загрузка в Gold (Postgres)
    Transformation_Block --> WriteStaging[5. Запись в staging.таблицы через ADBC Batch]
    
    %% Финальный контроль и Атомарный своп
    WriteStaging --> ValidateGold[6. Валидация Staging: сверка итогов и ключей]
    ValidateGold --> AtomicSwap[7. Атомарная замена данных в gold.схеме]
    
    %% Завершение
    AtomicSwap --> UpdateMeta[8. Обновление Watermark и статусов]
    UpdateMeta --> Finish((Финиш))

    %% Обработка сбоев
    ValidateSilver -- Ошибка --> FailDAG[AirflowFailException]
    ValidateGold -- Ошибка --> FailDAG
```

```mermaid
graph TD
    subgraph Step_5_Loading [Шаг 5: Загрузка в Staging]
        P[Polars LazyFrame] -- ADBC Batch Ingest --> ST[staging.mart_production_tmp]
    end

    subgraph Step_6_Validation [Шаг 6: Проверка]
        ST --> QC{Данные в порядке?}
        QC -- НЕТ --> FAIL[Остановка пайплайна. Gold не задет]
    end

    subgraph Step_7_Atomic_Swap [Шаг 7: Атомарная замена в транзакции]
        QC -- ДА --> BEGIN[BEGIN TRANSACTION]
        BEGIN --> DEL[Удалить старую партицию из gold.mart]
        DEL --> INS[Вставить данные из staging в gold.mart]
        INS --> COMMIT[COMMIT TRANSACTION]
    end

    COMMIT --> Finish((Данные обновлены в BI))
```

---

## 6. Руководство по развертыванию и запуску

Платформа полностью контейнеризирована и разворачивается с помощью **Docker Compose**.

### 6.1. Системные требования
*   **ОС:** Linux (рекомендуется) или macOS/Windows с установленным Docker Desktop.
*   **Ресурсы:** Минимум 8 ГБ ОЗУ (рекомендуется 16 ГБ), 4 ядра ЦП.
*   **Инструменты:** Docker 20.10+, Docker Compose 2.0+.

### 6.2. Шаг 1: Подготовка окружения
Клонируйте репозиторий и создайте необходимые директории для хранения данных:

```bash
git clone https://github.com/your-repo/oil-analytics-platform.git
cd oil-analytics-platform
mkdir -p ./dags ./logs ./plugins ./data/minio
```

### 6.3. Шаг 2: Запуск инфраструктуры
Запустите все сервисы в фоновом режиме. Docker Compose автоматически поднимет PostgreSQL, MinIO, Airflow, Superset и Jupyter.

```bash
docker-compose up -d --build
```

### 6.4. Шаг 3: Инициализация баз данных и схем
После запуска PostgreSQL необходимо создать структуру таблиц. Скрипты инициализации (`/scripts/init_db.sql`) обычно выполняются автоматически при старте контейнера, но при необходимости их можно запустить вручную:

```bash
# Вход в контейнер Postgres и создание схем
docker exec -it postgres psql -U admin -d company_data -f /docker-entrypoint-initdb.d/init_db.sql
```

### 6.5. Шаг 4: Настройка подключений в Airflow
Зайдите в UI Airflow (`http://localhost:8080`, логин/пароль по умолчанию: `airflow/airflow`). Подключение обычно устанавливаються автоматически, но при необходимости можно добавить следующие подключения (Connections):

1.  **postgres_default:** Тип `Postgres`, хост `postgres`, порт `5432`, БД `company_data`.
2.  **aws_default (для MinIO):**
    *   Conn Type: `Amazon S3`
    *   Extra: `{"endpoint_url": "http://minio:9000", "aws_access_key_id": "admin", "aws_secret_access_key": "password"}`

### 6.6. Шаг 5: Запуск пайплайнов
В интерфейсе Airflow включите (Unpause) и запустите DAG-и в следующей последовательности:
1.  `postgres_to_minio_enterprise_2` — загрузка из источника в Bronze слой.
2.  `bronze_to_silver_pipeline` — обработка, очистка и загрузка в Silver слой.
3.  `silver_to_gold_marts` — агрегация и публикация финальных витрин в Gold слой.

### 6.7. Шаг 6: Доступ к результатам

| Сервис | URL | Назначение |
| :--- | :--- | :--- |
| **Airflow** | `http://localhost:8080` | Мониторинг и управление пайплайнами. |
| **MinIO** | `http://localhost:9001` | Просмотр файлов в слоях Bronze, Silver, Quarantine. |
| **Superset** | `http://localhost:8088` | Визуализация дашбордов и анализ витрин. |
| **Jupyter** | `http://localhost:8888` | Исследование данных (EDA) и запуск ML-моделей. |





