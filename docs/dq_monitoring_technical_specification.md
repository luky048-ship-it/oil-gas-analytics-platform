**Техническая спецификация DAG `dq_monitoring_pipeline`**

---

## 1. Цель и область применения

DAG `dq_monitoring_pipeline` — это корневой конвейер качества данных (Data Quality Foundation Layer), обеспечивающий:

- 7-уровневую валидацию поступающих из Raw/Bronze датасетов перед загрузкой в Silver Zone;
- централизованную регистрацию результатов DQ в метаданных PostgreSQL;
- изоляцию «плохих» записей в Quarantine Zone;
- блокировку downstream‑мартовых пайплайнов при критических нарушениях.

Обрабатываются многомиллионные объёмы телеметрии с гарантией Enterprise‑grade: идемпотентность, ленивые вычисления, потоковая запись без полной материализации в памяти, отсутствие pandas, строгое соблюдение контрактов схем.

---

## 2. Архитектурный обзор

DAG выступает исключительно оркестратором. Трансформационная логика вынесена в переиспользуемые сервисные модули. Используется **единственный физический проход по данным** (single scan) с разветвлением результатов на Silver и Quarantine.

```
┌───────────┐
│  Airflow  │ (DAG – orchestrator only)
└─────┬─────┘
      │ вызывает
      ▼
┌─────────────────────────────────────────────────┐
│             Validation Services                 │
│  (модульные Python-функции в plugins/dq_utils) │
└──┬───┬───┬───┬───┬───┬───┬───┬───┬────────────┘
   │   │   │   │   │   │   │   │   │
   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
┌──────────┐  ┌──────────┐  ┌───────────────┐
│  MinIO   │  │PostgreSQL│  │  Silver Zone  │
│  (S3)    │  │ (metadata│  │  (valid data) │
│ Raw/     │  │  marts)  │  └───────────────┘
│ Quarant. │  └──────────┘
└──────────┘
```

---

## 3. Граф зависимостей и файловая структура

### 3.1. Список модулей (все файлы *.py)

| Файл | Назначение |
|------|------------|
| `dags/dq_monitoring_pipeline.py` | Определение Airflow DAG, вызов всех тасок |
| `plugins/dq_utils/__init__.py` | Пакет DQ-сервисов |
| `plugins/dq_utils/config.py` | Параметры подключений, контракты схем (expected_schema dicts), бизнес-правила, пороги |
| `plugins/dq_utils/s3_utils.py` | `get_s3_storage_options`, `discover_available_partitions`, `validate_file_integrity` |
| `plugins/dq_utils/schema_validator.py` | `validate_schema_contract` |
| `plugins/dq_utils/business_validator.py` | `validate_business_rules`, `validate_null_thresholds`, `validate_duplicate_keys` |
| `plugins/dq_utils/reference_validator.py` | `validate_reference_integrity` (анти-join с измерениями) |
| `plugins/dq_utils/freshness_validator.py` | `validate_data_freshness` |
| `plugins/dq_utils/statistical_validator.py` | `validate_distribution_drift`, `validate_volume_anomaly` |
| `plugins/dq_utils/quarantine_writer.py` | `write_quarantine_dataset` |
| `plugins/dq_utils/dq_reporter.py` | Класс `DQResult`, `persist_dq_results` (батч-запись в PostgreSQL) |
| `plugins/dq_utils/pipeline_status.py` | `publish_pipeline_status` |
| `plugins/dq_utils/core.py` | `execute_dq_pipeline` – единая точка обработки: загрузка LazyFrame, построение плана валидации, единственный collect, запись Silver/Quarantine, возврат списка DQResult |

### 3.2. Внешние зависимости графа

- **Airflow** → Hooks: `PostgresHook` (etl_metadata), `BaseHook` (S3/MinIO Connection).
- **S3/MinIO** → Raw/Bronze-партиции (Parquet), выход Quarantine.
- **PostgreSQL** → схемы `etl_metadata`, `gold.mart_*`.
- **Silver Zone** → валидированные паркеты (выход DAG).

---

## 4. Стек библиотек

```python
from __future__ import annotations
import logging, os
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import s3fs

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
import pyarrow.parquet as pq    # только для низкоуровневой проверки файлов
```

Запрещены: pandas, dask, pyspark.

---

## 5. Сигнатуры ключевых функций

### 5.1. `plugins/dq_utils/s3_utils.py`

```python
def get_s3_storage_options(conn_id: str = "s3_default") -> dict:
    """
    Извлекает ключи доступа из Airflow Connection и возвращает storage_options для Polars/s3fs.
    """

def discover_available_partitions(
    dataset: str,
    execution_date: str,
    s3_options: dict,
    base_path: str = "s3://datalake/raw"
) -> List[str]:
    """
    Проверяет существование партиций Parquet.
    Возвращает список полных S3-путей к найденным корректным партициям.
    Выбрасывает AirflowFailException при дубликатах или повреждённых файлах.
    """

def validate_file_integrity(
    partition_path: str,
    s3_options: dict
) -> DQResult:
    """
    Проверяет: файл существует, parquet читаем, не пуст, отсутствие повреждений.
    """
```

### 5.2. `plugins/dq_utils/schema_validator.py`

```python
def validate_schema_contract(
    lf: pl.LazyFrame,
    expected_schema: Dict[str, str],   # column_name -> expected_dtype
    dataset: str
) -> DQResult:
    """
    Сверяет фактические колонки и типы с контрактом.
    Проверяет отсутствующие, лишние колонки, несоответствие типов, nullable policy.
    Использует только LazyFrame.schema (без collect).
    """
```

### 5.3. `plugins/dq_utils/business_validator.py`

```python
def validate_null_thresholds(
    lf: pl.LazyFrame,
    thresholds: Dict[str, float]   # column -> max allowed null fraction
) -> DQResult:
    """
    Лениво вычисляет процент NULL для каждой колонки через агрегации.
    """

def validate_duplicate_keys(
    lf: pl.LazyFrame,
    key_columns: List[str],
    dataset: str
) -> DQResult:
    """
    Проверка уникальности бизнес-ключей. Ленивая группировка и фильтр count > 1.
    """

def validate_business_rules(
    lf: pl.LazyFrame,
    dataset: str
) -> List[DQResult]:
    """
    Применяет доменные ограничения (pressure > 0, downtime_hours <= 24 и т.д.)
    Возвращает список DQResult по каждому правилу.
    Использует добавление вычисляемых колонок-флагов в LazyFrame.
    """
```

### 5.4. `plugins/dq_utils/reference_validator.py`

```python
def validate_reference_integrity(
    lf_child: pl.LazyFrame,
    lf_parent: pl.LazyFrame,
    child_key: str,
    parent_key: str,
    dataset: str
) -> DQResult:
    """
    Орфан-записи через anti-join (how='anti').
    lf_parent загружается один раз из Silver-слоя.
    """
```

### 5.5. `plugins/dq_utils/freshness_validator.py`

```python
def validate_data_freshness(
    dataset: str,
    partition_date: str,
    max_delay_minutes: int,
    s3_options: dict
) -> DQResult:
    """
    Проверяет наличие партиции за ожидаемую дату и отставание по времени создания.
    Не требует загрузки данных.
    """
```

### 5.6. `plugins/dq_utils/statistical_validator.py`

```python
def validate_volume_anomaly(
    dataset: str,
    current_count: int,
    historical_avg: float,
    threshold_std: float = 3.0
) -> DQResult:
    """
    Z-score по количеству строк относительно исторической статистики из etl_metadata.
    """

def validate_distribution_drift(
    lf: pl.LazyFrame,
    monitored_columns: List[str],
    historical_stats: Dict[str, Tuple[float, float]]   # col -> (mean, std)
) -> List[DQResult]:
    """
    Лениво вычисляет средние и стандартные отклонения, сравнивает с историческими.
    Возвращает список DQResult с указанием дрейфа.
    """
```

### 5.7. `plugins/dq_utils/quarantine_writer.py`

```python
def write_quarantine_dataset(
    invalid_df: pl.DataFrame,          # уже отфильтрованные невалидные строки
    dataset: str,
    validation_name: str,
    partition_date: str,
    base_path: str = "s3://datalake/quarantine"
) -> str:
    """
    Записывает DataFrame в Quarantine Zone с разбиением partition_date=YYYY-MM-DD.
    Добавляет сервисные колонки: __reason_code, __validation_name, __execution_date.
    Возвращает путь.
    """
```

### 5.8. `plugins/dq_utils/dq_reporter.py`

```python
@dataclass
class DQResult:
    dataset: str
    validation_type: str
    status: str       # PASS, FAIL, WARNING
    failed_rows: int
    checked_rows: int
    message: str
    created_at: datetime

def persist_dq_results(
    results: List[DQResult],
    execution_date: str,
    postgres_conn_id: str = "postgres_metadata"
) -> None:
    """
    UPSERT (ON CONFLICT DO UPDATE) в etl_metadata.dq_validation_results.
    Использует batch-insert через execute_values.
    """
```

### 5.9. `plugins/dq_utils/pipeline_status.py`

```python
def publish_pipeline_status(
    dataset: str,
    execution_date: str,
    status: str,        # SUCCESS, FAILED, BLOCKED
    postgres_conn_id: str = "postgres_metadata"
) -> None:
    """
    Записывает итоговый статус в etl_metadata.dq_pipeline_runs.
    """
```

### 5.10. `plugins/dq_utils/core.py` – ключевой компонент

```python
def execute_dq_pipeline(
    dataset: str,
    partition_path: str,
    expected_schema: Dict[str, str],
    key_columns: List[str],
    parent_joins: List[Dict],          # [{child_key, parent_path, parent_key}]
    business_rules_config: Dict,
    historical_stats: Dict,
    execution_date: str,
    s3_options: dict
) -> Tuple[List[DQResult], pl.DataFrame, pl.DataFrame]:
    """
    Единый проход:
    1. Загружает LazyFrame через scan_parquet.
    2. Строит единый граф валидации (все проверки добавляются как колонки-флаги).
    3. Выполняет один collect() -> полный DataFrame с флагами.
    4. Рассчитывает агрегаты для DQ результатов.
    5. Разделяет DataFrame на valid и invalid.
    6. Возвращает список DQResult, valid_df, invalid_df.
    """
```

---

## 6. Реализация DAG в Airflow

### 6.1. Параметры DAG

```python
with DAG(
    dag_id="dq_monitoring_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "sla": timedelta(minutes=30),
        "pool": "dq_pool",
        "trigger_rule": "all_success",
    },
    tags=["data_quality", "silver_foundation"],
) as dag:
```

### 6.2. Задачи DAG (TaskFlow API)

```
start
  │
  ├─► get_connections (PythonOperator)
  │     └► возвращает s3_options
  │
  ├─► discover_partitions(dataset, execution_date)
  │     └► на вход s3_options, возвращает list[partition_path]
  │
  ├─► validate_file_layer(partition_path)   (для каждого пути)
  │     └► возвращает DQResult
  │
  ├─► validate_freshness(dataset, execution_date)
  │     └► DQResult
  │
  ├─► load_reference_tables(dataset)   (для всех FK)
  │     └► возвращает dict со сканированными LazyFrame
  │
  ├─► dq_process_dataset  (основная тяжёлая задача)
  │     Вход: partition_path, expected_schema, business_rules, parent_joins, historical_stats
  │     Выполняет execute_dq_pipeline, получает results, valid_df, invalid_df
  │     Записывает valid_df -> Silver Zone (потоково sink_parquet)
  │     Записывает invalid_df -> Quarantine (write_quarantine_dataset)
  │     Возвращает список DQResult (сериализован в XCom как JSON)
  │
  ├─► persist_dq_results(results)
  │     └► batch UPSERT в etl_metadata.dq_validation_results
  │
  └─► publish_pipeline_status(dataset, execution_date, итоговый статус)
```

Критические ошибки (corrupted parquet, missing partitions, schema drift) вызывают `AirflowFailException`, останавливая пайплайн до устранения причины.

---

## 7. Согласованность со Schema Contracts

- Все проверки Layer 2 (schema validation) базируются на `expected_schema`, извлечённом из контракта.
- Бизнес-правила (Layer 3) реализуют ограничения `Constraints` из разделов 5.1–5.17 контракта.
- Referential Integrity (Layer 4) использует явную матрицу `Referential Integrity Matrix` (раздел 6).
- Freshness‑проверки соответствуют SLA каждой таблицы.
- Статистический дрейф отслеживает метрики, заданные в разделе 7 (Observability).
- Все выходные таблицы и Quarantine‑записи сохраняют типы и единицы измерения, определённые контрактом.

---

## 8. Реализация 7 слоёв качества данных

### Layer 1 – File Validation
Функция `validate_file_integrity` через `s3fs` и `pyarrow.parquet` проверяет физическую целостность без сканирования данных.

### Layer 2 – Schema Validation
`validate_schema_contract` работает исключительно с метаданными `LazyFrame.schema`, без collect.

### Layer 3 – Business Validation
Встраивается в общий ленивый план: для каждого правила добавляется колонка `is_valid_<rule>` (например, `is_valid_pressure = pl.col("pressure") > 0`). Результаты агрегируются подсчётом неудач.

### Layer 4 – Referential Integrity
Анти‑join в ленивом режиме: `lf_child.join(lf_parent, left_on=child_key, right_on=parent_key, how="anti")`. Ключи‑сироты помечаются отдельным флагом.

### Layer 5 – Freshness
Проверка на уровне метаданных: S3‑листинг партиций без чтения данных.

### Layer 6 – Statistical Validation
- `validate_volume_anomaly` сверяет `current_count` (полученный ленивым `select(pl.count())`) с историческим средним из `etl_metadata`.
- `validate_distribution_drift` лениво вычисляет среднее и std для мониторимых колонок и сравнивает с эталонными `historical_stats`.

### Layer 7 – Completeness
- `validate_null_thresholds` использует ленивые агрегации `(col.is_null().sum() / pl.count()) * 100`.
- `validate_duplicate_keys` – ленивая группировка, фильтр `count > 1`, затем подсчёт дубликатов.

---

## 9. Механизм разветвления без двойной фильтрации

Единый план (в `execute_dq_pipeline`) добавляет итоговый флаг `__is_valid = all(is_valid_*) AND no_orphan AND ...`. Затем выполняется **один** `collect()`. Полученный DataFrame разделяется:

```python
collected_df = lf.collect(streaming=True)
valid_df = collected_df.filter(pl.col("__is_valid"))
invalid_df = collected_df.filter(~pl.col("__is_valid"))
```

Таким образом, партиция сканируется один раз, все проверки накладываются в одном DAG‑выражении, и запись Silver/Quarantine происходит из уже разделённых DataFrame без повторного чтения.

---

## 10. Идемпотентность и retry‑безопасность

- **Quarantine**: запись по пути `s3://datalake/quarantine/{dataset}/partition_date={execution_date}/` с перезаписью. Повторный запуск перезапишет те же невалидные данные.
- **Silver**: запись через `sink_parquet` с режимом `overwrite` на целевую партицию.
- **DQ‑отчёты**: UPSERT в `etl_metadata.dq_validation_results` по уникальному ключу `(dataset, validation_type, partition_date, execution_date)`. При конфликте обновляются метрики и `updated_at`.
- **Pipeline runs**: UPSERT в `dq_pipeline_runs` по `(dataset, partition_date)`.

---

## 11. Масштабирование и производительность

- Все операции построены на ленивых вычислениях Polars с predicate/projection pushdown.
- Партиционирование по `date`/`event_date` позволяет обрабатывать ежедневные окна, каждое – в отдельной задаче Airflow, горизонтально масштабируемой через пулы.
- Исторические статистики хранятся в PostgreSQL и подгружаются малым селектом.
- Анти‑join выполняется над родительскими LazyFrame, которые сканируются из компактных Silver‑паркетов (возможно, с использованием `cache`).
- Никаких полных обновлений или удалений больших таблиц.
