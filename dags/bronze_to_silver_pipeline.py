# dags/bronze_to_silver_pipeline.py
# =============================================================================
# ОПИСАНИЕ ПЛАЙПЛАЙНА: Bronze to Silver Pipeline
# =============================================================================
# Этот DAG (Directed Acyclic Graph) реализует процесс обработки данных 
# в архитектуре Medallion (Bronze → Silver). Он отвечает за трансформацию 
# сырых данных из Bronze-слоя в очищенные, проверенные данные Silver-слоя.
# 
# ОСНОВНЫЕ ФУНКЦИИ:
# - Инкрементальная обработка данных по watermark
# - Валидация схемы и бизнес-правил
# - Дедупликация записей
# - Обработка пропущенных значений и выбросов
# - Карантин некорректных данных
# - Обогащение справочными данными
# - Агрегация метрик
# - Управление метаданными и водяными знаками
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Импорты модулей валидации бизнес-правил
from bronze_to_silver.business_validator import validate_critical_rules
# Импорты конфигурации схем данных для каждого датасета
from bronze_to_silver.config import SCHEMA_CONTRACTS
# Импорты модуля дедупликации записей
from bronze_to_silver.deduplicator import deduplicate_dataset
# Импорты модуля обогащения справочными данными
from bronze_to_silver.enricher import enrich_reference_data
# Импорты модуля агрегации метрик по времени событий
from bronze_to_silver.event_time_aggregator import aggregate_event_time_metrics
# Импорты утилит работы с метаданными (watermark, публикация результатов)
from bronze_to_silver.metadata_utils import (get_last_watermark,
                                             publish_pipeline_metadata,
                                             update_pipeline_watermark)
# Импорты модуля обработки пропущенных значений
from bronze_to_silver.missing_handler import handle_missing_values
# Импорты модуля нормализации данных
from bronze_to_silver.normalizer import normalize_dataset
# Импорты модуля детекции выбросов
from bronze_to_silver.outlier_detector import detect_outliers
# Импорты модуля обнаружения инкрементальных партиций
from bronze_to_silver.partition_discovery import \
    discover_incremental_partitions
# Импорты класса результата выполнения пайплайна
from bronze_to_silver.pipeline_execution import PipelineExecutionResult
# Импорты модуля записи карантинных данных
from bronze_to_silver.quarantine_writer import write_quarantine_dataset
# Импорты утилит работы с S3 хранилищем
from bronze_to_silver.s3_utils import (get_s3_storage_options,
                                       load_bronze_dataset)
# Импорты модуля валидации схемы данных
from bronze_to_silver.schema_validator import validate_dataset_schema
# Импорты модуля записи данных в Silver-слой
from bronze_to_silver.silver_writer import write_silver_dataset

logger = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЙ И ПАРАМЕТРОВ
# =============================================================================
# ID подключения к PostgreSQL для хранения метаданных пайплайна
METADATA_CONN_ID = "postgres_default"

# Параметры по умолчанию для всех задач DAG
DEFAULT_ARGS = {
    "owner": "data-platform",              # Владелец пайплайна
    "retries": 2,                          # Количество повторных попыток при ошибке
    "retry_delay": timedelta(minutes=5),   # Задержка между попытками
    "execution_timeout": timedelta(hours=4),  # Максимальное время выполнения задачи
    "sla": timedelta(hours=2),             # Целевое время выполнения (SLA)
    "pool": "silver_processing",           # Пул ресурсов для ограничения параллелизма
}

# =============================================================================
# ОПРЕДЕЛЕНИЕ DAG (Directed Acyclic Graph)
# =============================================================================
# dag_id: Уникальный идентификатор пайплайна в Airflow
# schedule: Расписание выполнения - ежедневно (@daily)
# start_date: Дата начала выполнения пайплайна
# catchup: Флаг выполнения пропущенных запусков за прошлые периоды
# max_active_runs: Максимальное количество одновременных запусков (1 - последовательно)
# default_args: Параметры по умолчанию для всех задач
# tags: Теги для категоризации и поиска в интерфейсе Airflow
# doc_md: Автоматическая документация из docstring модуля
# =============================================================================
with DAG(
    dag_id="bronze_to_silver_pipeline",
    schedule="@daily",                    # Ежедневное выполнение
    start_date=datetime(2025, 10, 1),     # Дата первого запуска
    catchup=True,                         # Выполнять пропущенные запуски
    max_active_runs=1,                    # Только один активный запуск одновременно
    default_args=DEFAULT_ARGS,            # Применение настроек по умолчанию
    tags=["bronze", "silver", "medallion", "production"],  # Теги для классификации
    doc_md=__doc__,                       # Документация из заголовка файла
) as dag:

    # =============================================================================
    # БЛОК 1: ТОЧКИ ВХОДА И ВЫХОДА ПЛАЙПЛАЙНА
    # =============================================================================
    # start: Начальная точка выполнения DAG (пустая операция-маркер)
    # finish: Конечная точка выполнения DAG
    # trigger_rule="all_done": Задача выполняется даже если предыдущие задачи 
    #                          завершились с ошибкой (для сбора статистики)
    # =============================================================================
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_done")

    # =============================================================================
    # БЛОК 2: ЗАДАЧА ОТКРЫТИЯ ИНКРЕМЕНТАЛЬНЫХ ПАРТИЦИЙ
    # =============================================================================
    # discover_partitions: Функция для обнаружения новых партиций данных в S3
    # 
    # Параметры:
    #   - dataset: Имя обрабатываемого датасета
    #   - context: Контекст выполнения Airflow (содержит метаданные запуска)
    # 
    # Логика работы:
    #   1. Получение настроек подключения к S3 хранилищу
    #   2. Чтение последнего watermark (метка времени последней успешно 
    #      обработанной записи) из базы метаданных PostgreSQL
    #   3. Сканирование S3 для поиска новых партиций с данными, которые еще 
    #      не были обработаны (после watermark)
    #   4. Возврат списка путей к новым партициям для последующей обработки
    # 
    # Возвращаемое значение: list[str] - список путей к партициям в S3
    # =============================================================================
    @task(multiple_outputs=False)
    def discover_partitions(dataset: str, **context) -> list[str]:
        storage_options = get_s3_storage_options()
        watermark = get_last_watermark(dataset, conn_id=METADATA_CONN_ID)

        logger.info(f"Discovering partitions for {dataset} with watermark: {watermark}")
        partitions = discover_incremental_partitions(
            dataset=dataset, watermark=watermark, storage_options=storage_options
        )
        logger.info(f"Found {len(partitions)} new partitions for {dataset}.")
        return partitions

    # =============================================================================
    # БЛОК 3: ОСНОВНАЯ ЗАДАЧА ОБРАБОТКИ ДАННЫХ (process_dataset)
    # =============================================================================
    # process_dataset: Главная функция трансформации данных из Bronze в Silver
    # 
    # Параметры:
    #   - dataset: Имя обрабатываемого датасета
    #   - partition_paths: Список путей к партициям для обработки
    #   - context: Контекст выполнения Airflow (содержит execution_date и др.)
    # 
    # Возвращаемое значение: dict - словарь с результатами выполнения
    #   {"status": "skipped"|"success", "dataset": str, ...}
    # 
    # ЭТАПЫ ОБРАБОТКИ:
    #   1. Проверка наличия данных для обработки
    #   2. Загрузка данных из S3 Bronze-слоя
    #   3. Валидация схемы данных
    #   4. Нормализация данных
    #   5. Бизнес-валидация
    #   6. Дедупликация записей
    #   7. Обработка пропущенных значений
    #   8. Детекция выбросов
    #   9. Запись некорректных данных в карантин
    #   10. Агрегация метрик (если настроена)
    #   11. Обогащение справочными данными (если настроено)
    #   12. Запись очищенных данных в Silver-слой
    #   13. Обновление watermark и публикация метаданных
    # =============================================================================
    @task(multiple_outputs=False)
    def process_dataset(dataset: str, partition_paths: list[str], **context) -> dict:
        # -------------------------------------------------------------------------
        # ЭТАП 3.1: ПРОВЕРКА НАЛИЧИЯ ДАННЫХ ДЛЯ ОБРАБОТКИ
        # -------------------------------------------------------------------------
        # Если список партиций пуст - пропускаем обработку для этого датасета
        if not partition_paths:
            logger.info(f"No new partitions to process for {dataset}.")
            return {"status": "skipped", "dataset": dataset}

        t_start = datetime.now()  # Фиксация времени начала обработки
        execution_date = context["ds"]  # Дата выполнения (из контекста Airflow)
        storage_options = get_s3_storage_options()  # Настройки подключения к S3
        contract = SCHEMA_CONTRACTS[dataset]  # Получение контракта схемы для датасета
        watermark = get_last_watermark(dataset, conn_id=METADATA_CONN_ID)  # Последний watermark

        # -------------------------------------------------------------------------
        # ЭТАП 3.2: ЗАГРУЗКА ДАННЫХ ИЗ BRONZE-СЛОЯ S3
        # -------------------------------------------------------------------------
        # Загрузка данных из указанных партиций с применением фильтра по watermark
        # time_column: Столбец времени для фильтрации инкрементальных данных
        lf = load_bronze_dataset(
            dataset_paths=partition_paths,
            storage_options=storage_options,
            watermark=watermark,
            time_column=contract.get("time_column"),
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.3: ВАЛИДАЦИЯ СХЕМЫ ДАННЫХ
        # -------------------------------------------------------------------------
        # Проверка соответствия структуры данных контракту схемы
        # Выбрасывает исключение при несоответствии типов или отсутствии колонок
        validate_dataset_schema(lf, dataset, contract["columns"])
        
        # -------------------------------------------------------------------------
        # ЭТАП 3.4: НОРМАЛИЗАЦИЯ ДАННЫХ
        # -------------------------------------------------------------------------
        # Приведение данных к стандартному формату:
        # - Преобразование типов данных
        # - Нормализация имен колонок
        # - Стандартизация форматов дат и строк
        lf = normalize_dataset(lf, dataset, contract)

        # -------------------------------------------------------------------------
        # ЭТАП 3.5: БИЗНЕС-ВАЛИДАЦИЯ
        # -------------------------------------------------------------------------
        # Применение бизнес-правил валидации к данным
        # Возвращает два набора данных:
        #   - valid_lf: Данные, прошедшие все проверки
        #   - business_invalid_lf: Данные, не прошедшие бизнес-валидацию
        valid_lf, business_invalid_lf = validate_critical_rules(
            lf, contract.get("validation_rules", {})
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.6: ДЕДУПЛИКАЦИЯ ЗАПИСЕЙ
        # -------------------------------------------------------------------------
        # Удаление дубликатов на основе ключевых колонок
        # key_columns: Колонки для определения уникальности записи
        # timestamp_column: Колонка времени для выбора последней версии записи
        valid_lf = deduplicate_dataset(
            valid_lf,
            key_columns=contract.get("dedup_key"),
            timestamp_column=contract.get("time_column"),
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.7: ОБРАБОТКА ПРОПУЩЕННЫХ ЗНАЧЕНИЙ
        # -------------------------------------------------------------------------
        # Применение стратегий обработки NULL-значений:
        # - Заполнение дефолтными значениями
        # - Интерполяция
        # - Удаление записей с критическими пропусками
        valid_lf = handle_missing_values(
            valid_lf, dataset, contract.get("missing_rules", {})
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.8: ДЕТЕКЦИЯ ВЫБРОСОВ (OUTLIER DETECTION)
        # -------------------------------------------------------------------------
        # Обнаружение аномальных значений методом IQR (Interquartile Range)
        # multiplier: Коэффициент для определения границ выбросов (3.0 = 3 sigma)
        # Возвращает:
        #   - valid_lf: Данные без выбросов
        #   - outlier_invalid_lf: Записи, содержащие выбросы
        valid_lf, outlier_invalid_lf = detect_outliers(
            valid_lf,
            dataset,
            monitored_columns=contract.get("outlier_columns", []),
            method="iqr",
            multiplier=3.0,
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.9: ПОДГОТОВКА И ЗАПИСЬ ДАННЫХ В КАРАНТИН
        # -------------------------------------------------------------------------
        # Сбор всех некорректных данных (бизнес-валидация + выбросы) в один набор
        all_invalid_lfs = []
        if business_invalid_lf is not None:
            all_invalid_lfs.append(business_invalid_lf)
        if outlier_invalid_lf is not None:
            all_invalid_lfs.append(outlier_invalid_lf)

        # Нормализация схемы карантинных данных:
        # Добавление служебных колонок для трассировки причин попадания в карантин
        q_rows = 0
        normalized_lfs = []
        for invalid_lf_item in all_invalid_lfs:
            schema = invalid_lf_item.collect_schema()
            lf_with_meta = invalid_lf_item

            # Добавление колонки с именем валидации, если отсутствует
            if "_quarantine_validation_name" not in schema:
                lf_with_meta = lf_with_meta.with_columns(
                    pl.lit("UNKNOWN_VALIDATION").alias("_quarantine_validation_name")
                )
            # Добавление колонки с кодом причины, если отсутствует
            if "_quarantine_reason_code" not in schema:
                lf_with_meta = lf_with_meta.with_columns(
                    pl.lit("UNKNOWN_REASON").alias("_quarantine_reason_code")
                )

            normalized_lfs.append(lf_with_meta)

        # Объединение всех наборов некорректных данных в единый DataFrame
        final_invalid_lf = pl.concat(normalized_lfs)
        
        # Запись карантинных данных в отдельное хранилище для последующего анализа
        q_rows = write_quarantine_dataset(
            invalid_lf=final_invalid_lf,
            dataset=dataset,
            reason_code="DQ_VIOLATION",  # Код причины: нарушение качества данных
            execution_date=execution_date,
            storage_options=storage_options,
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.10: АГРЕГАЦИЯ МЕТРИК (ЕСЛИ НАСТРОЕНА В КОНТРАКТЕ)
        # -------------------------------------------------------------------------
        # Применение агрегаций к данным (суммы, средние, количества и т.д.)
        # Конфигурация агрегации определяется в контракте датасета
        if "aggregation" in contract:
            valid_lf = aggregate_event_time_metrics(
                valid_lf, dataset, contract["aggregation"]
            )

        # -------------------------------------------------------------------------
        # ЭТАП 3.11: ОБОГАЩЕНИЕ СПРАВОЧНЫМИ ДАННЫМИ (ЕСЛИ НАСТРОЕНО)
        # -------------------------------------------------------------------------
        # Присоединение дополнительных данных из справочников (reference datasets)
        # Для каждого join-определения в контракте выполняется соединение
        if "joins" in contract:
            for join_def in contract["joins"]:
                valid_lf = enrich_reference_data(
                    lf=valid_lf,
                    reference_dataset=f"s3://datalake/silver/{join_def['ref_dataset']}",
                    join_key=join_def["key"],
                    storage_options=storage_options,
                    how=join_def.get("how", "left"),  # Тип соединения по умолчанию: left
                )

        # -------------------------------------------------------------------------
        # ЭТАП 3.12: ЗАПИСЬ ДАННЫХ В SILVER-СЛОЙ
        # -------------------------------------------------------------------------
        # Сохранение очищенных и трансформированных данных в целевую директорию
        # Silver-слоя с партиционированием по дате выполнения
        output_path = write_silver_dataset(
            lf=valid_lf,
            dataset=dataset,
            partition_date=execution_date,
            storage_options=storage_options,
        )

        # -------------------------------------------------------------------------
        # ЭТАП 3.13: ВЫЧИСЛЕНИЕ МЕТРИК И НОВОГО WATERMARK
        # -------------------------------------------------------------------------
        # Расчет количества обработанных строк и определение нового watermark
        # time_column: Столбец времени для отслеживания прогресса обработки
        time_col = contract.get("time_column")

        if time_col and "aggregation" not in contract:
            # Если есть столбец времени и агрегация не применялась,
            # вычисляем количество строк и максимальное значение времени
            metrics_df = valid_lf.select(
                [pl.len().alias("count"), pl.col(time_col).max().alias("max_time")]
            ).collect()

            processed_rows = metrics_df["count"].item(0)
            new_watermark = metrics_df["max_time"].item(0)
        else:
            # Для агрегированных данных или без time_column
            # вычисляем только количество строк
            processed_rows = valid_lf.select(pl.len()).collect().item()
            new_watermark = None

        # Если watermark не был определен, используем предыдущий или дату выполнения
        if not new_watermark:
            new_watermark = watermark or datetime.strptime(execution_date, "%Y-%m-%d")

        t_end = datetime.now()  # Фиксация времени окончания обработки
        execution_time = (t_end - t_start).total_seconds()  # Расчет времени выполнения

        # Создание объекта результата выполнения пайплайна
        result = PipelineExecutionResult(
            dataset=dataset,
            partition_date=execution_date,
            processed_rows=processed_rows,
            quarantined_rows=q_rows,
            output_path=output_path,
            execution_time_sec=execution_time,
            watermark=new_watermark,
        )

        # Обновление watermark в базе метаданных для следующего запуска
        update_pipeline_watermark(
            dataset, new_watermark, execution_date, conn_id=METADATA_CONN_ID
        )
        # Публикация метаданных о выполнении пайплайна
        publish_pipeline_metadata(result, conn_id=METADATA_CONN_ID)

        logger.info(
            f"Successfully processed {dataset}: {processed_rows} rows. Watermark advanced to {new_watermark}."
        )
        return result.__dict__

    # =============================================================================
    # БЛОК 4: ДИНАМИЧЕСКОЕ СОЗДАНИЕ ЗАДАЧ ДЛЯ КАЖДОГО ДАТАСЕТА
    # =============================================================================
    # Для каждого датасета, определенного в SCHEMA_CONTRACTS:
    #   1. Создается отдельная TaskGroup с уникальным идентификатором
    #   2. Внутри группы создаются две связанные задачи:
    #      - discover_partitions: Обнаружение новых партиций
    #      - process_dataset: Обработка обнаруженных партиций
    #   3. Задачи связываются зависимостью: discover_partitions >> process_dataset
    #   4. Группа задач подключается к основному потоку DAG: start >> tg >> finish
    # 
    # Такая архитектура позволяет:
    #   - Параллельно обрабатывать несколько независимых датасетов
    #   - Изолировать ошибки обработки одного датасета от других
    #   - Легко добавлять новые датасеты через конфигурацию SCHEMA_CONTRACTS
    # =============================================================================
    for ds_name in SCHEMA_CONTRACTS.keys():
        with TaskGroup(group_id=f"process_group_{ds_name}") as tg:

            # Создание задачи обнаружения партиций с уникальным task_id
            discovered_paths = discover_partitions.override(
                task_id=f"discover_{ds_name}"
            )(dataset=ds_name)

            # Создание задачи обработки dataset с уникальным task_id
            # Получает пути к партициям из предыдущей задачи
            processed_result = process_dataset.override(task_id=f"process_{ds_name}")(
                dataset=ds_name, partition_paths=discovered_paths
            )

            # Установка зависимости между задачами внутри группы
            discovered_paths >> processed_result

        # Подключение группы задач к основному потоку выполнения DAG
        start >> tg >> finish
