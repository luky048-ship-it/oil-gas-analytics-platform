## Техническая спецификация DAG `bronze_to_silver_pipeline`  
### Платформа аналитики нефтедобычи – Enterprise Bronze → Silver слой

---

## 1. Общая архитектура и назначение

`bronze_to_silver_pipeline` – это **единственный контролируемый шлюз** между неизменяемым Raw‑слоем (Bronze) и аналитически готовым Silver‑слоем.  
DAG обеспечивает:

* потоковую (инкрементальную) обработку телеметрии и мастер-данных;
* полную валидацию schema contract, качества и ссылочной целостности;
* нормализацию, дедупликацию, интерполяцию пропусков;
* выявление и изоляцию аномалий в зону карантина;
* событийно‑временную агрегацию (daily‑гранулы);
* обогащение справочными данными;
* атомарную, повторяемую запись партиционированного Parquet в Silver;
* обновление watermark и публикацию метаданных для downstream‑оркестрации.

**Целевое состояние**: Idempotent, retry‑safe, metadata‑driven пайплайн, готовый для автоматического масштабирования и бесшовной интеграции с ML‑конвейерами и BI‑витринами.

---

## 2. Граф зависимостей (файлы и модули)

DAG реализован как Airflow‑оркестратор, использующий легковесные Python‑модули трансформаций на Polars.  
Структура репозитория:

```
dags/
  bronze_to_silver_pipeline.py          # DAG-определение (Airflow)
  
plugins/
  bronze_to_silver/
    __init__.py
    config.py                           # Контракты схем, правила валидации, настройки
    s3_utils.py                         # S3‑клиент, storage options
    metadata_utils.py                   # PostgreSQL‑хелперы: watermark, publish
    partition_discovery.py              # Инкрементальный поиск партиций
    schema_validator.py                 # Валидация контракта схемы
    normalizer.py                       # Нормализация типов, таймстемпов, единиц
    deduplicator.py                     # Дедупликация по event‑time
    missing_handler.py                  # Обработка пропусков
    outlier_detector.py                 # Статистический поиск выбросов
    quarantine_writer.py                # Запись в S3‑карантин
    event_time_aggregator.py            # Агрегация по окнам событийного времени
    enricher.py                         # Обогащение справочными данными
    silver_writer.py                    # Запись в Silver‑партиции
    pipeline_execution.py               # Dataclass PipelineExecutionResult
```

Все модули содержат **только чистые функции**, не имеющие побочных эффектов, кроме взаимодействия с хранилищем (S3/Postgres). Airflow DAG содержит вызовы этих функций через `@task`‑декораторы или `PythonOperator`.

---

## 3. Стек обязательных библиотек

```python
# стандартные
from __future__ import annotations
import os, logging, json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

# обработка данных – только lazy
import polars as pl

# S3-доступ
import s3fs
import pyarrow.dataset as ds   # для partition discovery, write_dataset
import pyarrow as pa

# Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from psycopg2.extras import execute_values

# дополнительно
import numpy as np             # только для статистики (без pandas)
from scipy import stats        # для z‑score (опционально)
```

Запрещены: `pandas`, `pyspark`, `dask`, любое eager‑чтение полного датасета.

---

## 4. Структура DAG (Airflow)

```python
with DAG(
    dag_id='bronze_to_silver_pipeline',
    schedule_interval='@daily',         # запуск раз в сутки в 00:00 UTC
    start_date=datetime(2024,1,1),
    catchup=True,                       # обязательная обработка пропущенных окон
    max_active_runs=1,                  # идемпотентность
    default_args={
        'owner': 'data-platform',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=4),
        'sla': timedelta(hours=2),
        'pool': 'silver_processing',
    },
    tags=['bronze', 'silver', 'medallion'],
) as dag:

    @task
    def start():
        ...

    @task
    def discover_partitions(dataset: str, **context) -> List[str]:
        ...

    @task
    def process_dataset(dataset: str, partitions: List[str], **context) -> PipelineExecutionResult:
        ...

    @task
    def finish():
        ...

    # Динамическая генерация задач для каждого dataset
    datasets = [
        'production', 'well_telemetry', 'well_targets',
        'pump_sensors', 'pump_failures',
        'deliveries', 'drivers', 'vehicles',
        'wells', 'pumps', 'oil_stations'   # мастер-данные обрабатываются при необходимости
    ]

    for ds_name in datasets:
        parts = discover_partitions.override(task_id=f'discover_{ds_name}')(ds_name)
        processed = process_dataset.override(task_id=f'process_{ds_name}', retries=1)(ds_name, parts)
        start() >> parts >> processed >> finish()
```

**Ключевые моменты**:
* `process_dataset` инкапсулирует полную цепочку шагов Bronze→Silver (validate → normalize → ... → write).
* Каждый dataset обрабатывается параллельно (доступные слоты в pool).
* Задачи внутри `process_dataset` реализованы **как последовательные вызовы функций** внутри одного PythonOperator, что исключает накладные расходы на XCom с большими данными.

---

## 5. Сигнатуры функций (core services)

Все функции размещены в соответствующих модулях `plugins/bronze_to_silver/`.  
Приведены **только публичные сигнатуры**, реализующие ключевые шаги.

### 5.1 S3‑конфигурация
```python
# plugins/bronze_to_silver/s3_utils.py
def get_s3_storage_options() -> dict:
    """
    Получает credentials из Airflow Connection 's3_datalake'.
    Возвращает словарь для передачи в storage_options Polars/pyarrow.
    """
```

### 5.2 Работа с watermark
```python
# plugins/bronze_to_silver/metadata_utils.py
def get_last_watermark(dataset: str) -> Optional[datetime]:
    """
    Читает etl_metadata.pipeline_watermarks для dataset.
    Возвращает последний обработанный event_time (максимальный timestamp).
    """

def update_pipeline_watermark(
    dataset: str, watermark: datetime, execution_date: str
) -> None:
    """
    Атомарно обновляет watermark (UPSERT по dataset).
    """
```

### 5.3 Partition discovery
```python
# plugins/bronze_to_silver/partition_discovery.py
def discover_incremental_partitions(
    dataset: str,
    watermark: datetime,
    bronze_base: str = "s3://datalake/raw",
) -> List[str]:
    """
    Использует pyarrow.dataset для сканирования структуры 
    .../bronze/{dataset}/partition_date=YYYY-MM-DD/.
    Возвращает полные S3-пути партиций, у которых partition_date > watermark.
    """
```

### 5.4 Загрузка Bronze
```python
# plugins/bronze_to_silver/s3_utils.py (или отдельный reader)
def load_bronze_dataset(
    dataset_paths: List[str],
    storage_options: dict,
) -> pl.LazyFrame:
    """
    pl.scan_parquet по списку путей – без материализации.
    Predicate pushdown: добавляет фильтр по event_date >= watermark автоматически.
    """
```

### 5.5 Валидация схемы
```python
# plugins/bronze_to_silver/schema_validator.py
def validate_dataset_schema(
    lf: pl.LazyFrame,
    dataset: str,
    expected_schema: Dict[str, str],
) -> None:
    """
    Сравнивает фактическую схему lf с контрактом из config.py.
    При несовпадении имён колонок или типов – AirflowFailException.
    При добавлении новых nullable колонок – warning.
    """
```

### 5.6 Нормализация
```python
# plugins/bronze_to_silver/normalizer.py
def normalize_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    schema_contract: Dict,      # полное описание из config
) -> pl.LazyFrame:
    """
    Приводит:
      - timestamp(s) к UTC, обрезает до секунд;
      - float64 – проверяет NaN/Inf, приводит диапазоны;
      - enum поля приводятся к lowercase, валидируются;
      - добавляет техническую колонку _silver_processed_at.
    Возвращает LazyFrame без материализации.
    """
```

### 5.7 Дедупликация
```python
# plugins/bronze_to_silver/deduplicator.py
def deduplicate_dataset(
    lf: pl.LazyFrame,
    key_columns: List[str],
    timestamp_column: str,
) -> pl.LazyFrame:
    """
    Оставляет только последнюю запись в рамках группы (key_columns), 
    используя row_number() over (partition by key_columns order by timestamp_column desc).
    Реализовано через оконные функции Polars (lazy).
    """
```

### 5.8 Обработка пропусков
```python
# plugins/bronze_to_silver/missing_handler.py
def handle_missing_values(
    lf: pl.LazyFrame,
    dataset: str,
    rules: Dict,                # кастомные правила интерполяции
) -> pl.LazyFrame:
    """
    Для телеметрических колонок – forward fill в пределах окна well_id/pump_id.
    Для справочных полей – значение по умолчанию 'UNKNOWN'.
    """
```

### 5.9 Детекция выбросов
```python
# plugins/bronze_to_silver/outlier_detector.py
def detect_outliers(
    lf: pl.LazyFrame,
    dataset: str,
    monitored_columns: List[str],
    method: str = 'iqr',            # iqr или zscore
    multiplier: float = 3.0,
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Возвращает два LazyFrame:
      valid_lf   – без выбросов;
      invalid_lf – строки с причиной, обогащённые reason_code, validation_name.
    Выбросы определяются статистически (IQR / z‑score) без удаления silently.
    """
```

### 5.10 Запись в карантин
```python
# plugins/bronze_to_silver/quarantine_writer.py
def write_quarantine_dataset(
    invalid_lf: pl.LazyFrame,
    dataset: str,
    reason_code: str,
    execution_date: str,
    base_path: str = "s3://datalake/quarantine",
) -> str:
    """
    Сохраняет invalid_lf в 
    .../quarantine/{dataset}/partition_date=YYYY-MM-DD/ 
    с добавлением служебных колонок (validation_name, reason_code, execution_date, source_partition).
    """
```

### 5.11 Агрегация по событийному времени
```python
# plugins/bronze_to_silver/event_time_aggregator.py
def aggregate_event_time_metrics(
    lf: pl.LazyFrame,
    dataset: str,
    aggregation_rules: Dict,       # набор агрегаций для каждого dataset
) -> pl.LazyFrame:
    """
    Группирует по well_id (или pump_id) и event_date (daily гранула).
    Учитывает late arriving data: агрегация только по строкам с event_time >= watermark.
    """
```

### 5.12 Обогащение справочниками
```python
# plugins/bronze_to_silver/enricher.py
def enrich_reference_data(
    lf: pl.LazyFrame,
    reference_dataset: str,         # путь к справочнику в Silver
    join_key: str,
    how: str = 'left',
) -> pl.LazyFrame:
    """
    Lazy join с заранее материализованным справочником (wells, pumps, drivers, vehicles).
    Справочники читаются через scan_parquet с фильтром по последней версии.
    """
```

### 5.13 Запись в Silver
```python
# plugins/bronze_to_silver/silver_writer.py
def write_silver_dataset(
    lf: pl.LazyFrame,
    dataset: str,
    partition_date: str,            # YYYY-MM-DD из контекста выполнения
    silver_base: str = "s3://datalake/silver",
) -> str:
    """
    Записывает LazyFrame в 
    .../silver/{dataset}/partition_date=YYYY-MM-DD/
    с mode='overwrite' для идемпотентности.
    Использует pyarrow.dataset.write_dataset для атомарной записи.
    """
```

### 5.14 Публикация метаданных
```python
# plugins/bronze_to_silver/metadata_utils.py
def publish_pipeline_metadata(
    result: PipelineExecutionResult,
) -> None:
    """
    UPSERT в etl_metadata.pipeline_executions:
      - dataset, execution_date, processed_rows, quarantined_rows,
        execution_time_sec, watermark, status='SUCCESS'
    """
```

### 5.15 Dataclass результата
```python
# plugins/bronze_to_silver/pipeline_execution.py
@dataclass
class PipelineExecutionResult:
    dataset: str
    partition_date: str
    processed_rows: int
    quarantined_rows: int
    output_path: str
    execution_time_sec: float
    watermark: datetime
```

---

## 6. Детализация ключевых модулей

### 6.1 Конфигурация схем и правил (`config.py`)

Централизованный реестр schema contracts.  
Для каждого dataset содержит:

```python
SCHEMA_CONTRACTS = {
    "production": {
        "columns": {
            "prod_id": "int32",
            "well_id": "int32",
            "date": "date32",
            "oil_ton": "float64",
            ...
        },
        "primary_key": "prod_id",
        "foreign_keys": {"well_id": "wells.well_id"},
        "validation_rules": [
            {"rule": "prod_id unique", "severity": "CRITICAL"},
            {"rule": "well_id exists in wells", "severity": "CRITICAL"},
            {"rule": "oil_ton >= 0", "severity": "HIGH"},
            ...
        ],
        "outlier_columns": ["oil_ton", "gas_m3", "pressure", ...],
        "aggregation": {
            "key": "well_id",
            "time_column": "date",
            "metrics": {"oil_ton": "sum", "downtime_hours": "sum", ...}
        },
        "dedup_key": ["well_id", "date"],   # для дедупликации после агрегации
    },
    "well_telemetry": { ... },
    ...
}
```

Валидация выполняется динамически на основе этих правил.

### 6.2 Полный цикл обработки в `process_dataset`

```python
def process_dataset(dataset: str, partition_paths: List[str], **context) -> PipelineExecutionResult:
    t_start = datetime.now()
    execution_date = context['ds']  # YYYY-MM-DD
    storage_options = get_s3_storage_options()
    contract = SCHEMA_CONTRACTS[dataset]
    
    # 1. Load
    lf = load_bronze_dataset(partition_paths, storage_options)
    
    # 2. Schema contract validation
    validate_dataset_schema(lf, dataset, contract["columns"])
    
    # 3. Normalize
    lf = normalize_dataset(lf, dataset, contract)
    
    # 4. Deduplicate (by natural key + timestamp)
    lf = deduplicate_dataset(lf, 
                             key_columns=contract.get("dedup_key"), 
                             timestamp_column=contract.get("time_column", "date"))
    
    # 5. Handle missing values
    lf = handle_missing_values(lf, dataset, contract.get("missing_rules"))
    
    # 6. Outlier detection → split
    valid_lf, invalid_lf = detect_outliers(
        lf, dataset, monitored_columns=contract["outlier_columns"]
    )
    
    # 7. Quarantine invalid
    if invalid_lf is not None:
        q_rows = write_quarantine_dataset(
            invalid_lf, dataset, reason_code="STATISTICAL_OUTLIER",
            execution_date=execution_date
        )
    else:
        q_rows = 0
    
    # 8. Event-time aggregation (daily rollup)
    lf_agg = aggregate_event_time_metrics(valid_lf, dataset, contract["aggregation"])
    
    # 9. Enrich with reference data (wells, pumps, drivers etc.)
    if "joins" in contract:
        for join_def in contract["joins"]:
            lf_agg = enrich_reference_data(
                lf_agg, 
                reference_dataset=join_def["ref_dataset"],
                join_key=join_def["key"],
                how=join_def.get("how", "left")
            )
    
    # 10. Write Silver
    output_path = write_silver_dataset(lf_agg, dataset, partition_date=execution_date)
    
    # 11. Count rows (lazy collect только для метаданных)
    row_count = lf_agg.select(pl.len()).collect().item()
    
    # 12. Update watermark
    new_watermark = compute_max_event_time(lf_agg, contract)  # lazy collect только max
    update_pipeline_watermark(dataset, new_watermark, execution_date)
    
    t_end = datetime.now()
    result = PipelineExecutionResult(
        dataset=dataset,
        partition_date=execution_date,
        processed_rows=row_count,
        quarantined_rows=q_rows,
        output_path=output_path,
        execution_time_sec=(t_end - t_start).total_seconds(),
        watermark=new_watermark
    )
    
    # 13. Publish metadata to PostgreSQL
    publish_pipeline_metadata(result)
    
    return result
```

**Важно**: единственные `.collect()` вызовы – финальный подсчёт и `max(event_time)`. Основной датафрейм никогда не материализуется целиком.

### 6.3 Watermark и инкрементальность

* Таблица `etl_metadata.pipeline_watermarks`:
  ```sql
  CREATE TABLE etl_metadata.pipeline_watermarks (
      dataset VARCHAR(100) PRIMARY KEY,
      last_processed_watermark TIMESTAMP,
      updated_at TIMESTAMP DEFAULT NOW()
  );
  ```
* Watermark – максимальное значение `event_time` (или `date` для production) обработанных записей.
* `discover_incremental_partitions` проверяет, что `partition_date` бронзовой партиции > текущего watermark.
* При catchup (историческая загрузка) первый запуск для каждой партиции будет иметь watermark = минимальная дата, и последовательно обработает все окна.

### 6.4 Карантин (Quarantine)

Каждая quarantined‑строка получает обязательные атрибуты:
- `_quarantine_validation_name`: "OUTLIER_DETECTION" (или "SCHEMA_VIOLATION", если расширим).
- `_quarantine_reason_code`: "IQR_VIOLATION", "ZSCORE_VIOLATION" и т.п.
- `_quarantine_execution_date`: Airflow `ds`.
- `_quarantine_source_dataset`: исходный dataset.
- `_quarantine_source_partition`: исходная бронзовая партиция.

Партиция карантина: `s3://datalake/quarantine/{dataset}/partition_date=YYYY-MM-DD/`.

### 6.5 Silver‑запись (атомарность)

`write_silver_dataset` использует `pyarrow.dataset.write_dataset` с параметрами:
- `format='parquet'`
- `partitioning=ds.partitioning(field_names=['partition_date'])`
- `existing_data_behavior='overwrite_or_ignore'` (если партиция существует, перезаписываем – идемпотентность)
- `max_partitions=1024` – масштабируемость

Тем самым гарантируется атомарная замена целевой партиции без состояния «частично записанных файлов».

### 6.6 Интеграция с DQ Handoff (контракт для downstream)

Поскольку `bronze_to_silver_pipeline` выполняет полный цикл проверок и записывает **валидированные данные**, он же публикует в `etl_metadata.pipeline_executions` статус `SUCCESS` для каждой пары `(dataset, partition_date)`.  
Downstream DAG’и Gold‑слоя должны использовать `SqlSensor`:
```sql
SELECT status FROM etl_metadata.pipeline_executions
WHERE dataset = '{dataset}' AND partition_date = '{ds}'
```
Ожидая `SUCCESS`, тем самым они полагаются на предоставленный контракт:
- схема Silver точно соответствует заявленной;
- дубликаты, пропуски ключей, выбросы обработаны;
- ссылочная целостность гарантирована (FK не ведут в пустоту).

---

## 7. Обработка ошибок и идемпотентность

| Сценарий                                   | Поведение                                                                                     |
|--------------------------------------------|-----------------------------------------------------------------------------------------------|
| Schema drift (критическое несовпадение)    | `AirflowFailException`, DAG падает. Требуется ручное вмешательство.                           |
| Повреждённый Parquet в Bronze              | `AirflowFailException`, после retries — финальный fail.                                       |
| Частичная запись Silver (retry)            | Перезапись партиции `overwrite` гарантирует идемпотентность.                                  |
| Повторный запуск за ту же дату             | Watermark не изменится; `discover_partitions` вернёт пустой список, задача пропущена.         |
| Out of memory при агрегации?               | Polars streaming + эффективное использование partition pruning. Размер окна контролируется.   |

---

## 8. Масштабирование и performance

* **Polars streaming** – используется везде, где возможна потоковая обработка (все операции, кроме оконных, которые требуют сортировки, но они работают над уже агрегированными окнами).
* **Partition pruning** – `scan_parquet` с фильтром `partition_date >= watermark` гарантирует чтение только нужных файлов.
* **Параллелизм** – в Airflow каждый dataset обрабатывается в собственном task’е, пул `silver_processing` ограничивает параллелизм по вычислительным ресурсам (например, 4 слота).
* **Память** – `.collect()` вызывается только для метрик, основной датафрейм стримится прямо в S3‑writer.
* **Дедупликация больших окон** – использует `pl.LazyFrame.unique(subset=[...], keep='last')` с последующей сортировкой по ключам и времени; Polars выполняет её эффективно без загрузки всех данных в память.

---

## 9. Мониторинг и наблюдаемость

* Все метрики сохраняются в `etl_metadata.pipeline_executions` → доступны для дашбордов Superset.
* Логирование через стандартный `logging` с уровнями INFO/WARNING/ERROR.
* SLA в Airflow – 2 часа; при превышении алерт в систему мониторинга.
* DQ‑метрики (процент выбросов, дубликатов) доступны через отдельную витрину Gold при необходимости.

---

## 10. Заключение

Представленная спецификация описывает **enterprise‑ready, идемпотентный, масштабируемый пайплайн** для перехода от сырых данных к очищенному Silver‑слою.  
Архитектура следует принципам Medallion, использует исключительно ленивые вычисления, исключает полное сканирование и гарантирует контрактную целостность данных для всех downstream‑потребителей.

При реализации необходимо строго придерживаться указанных сигнатур функций, контрактов схем и подходов к обработке ошибок. Код должен быть покрыт интеграционными тестами, имитирующими чтение/запись на S3 (MinIO) и PostgreSQL.
