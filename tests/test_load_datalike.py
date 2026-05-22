import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from airflow.models import DagBag

from dags.loading_in_datalike_minio import (EXPECTED_SCHEMAS, TABLE_COLUMNS,
                                            TABLES_CONFIG,
                                            _cast_batch_to_schema,
                                            acquire_partition_lock,
                                            extract_load,
                                            release_partition_lock)

# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_postgres_hook(monkeypatch):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor

    mock_hook = MagicMock()
    mock_hook.get_conn.return_value = mock_conn

    mock_hook_class = MagicMock(return_value=mock_hook)
    monkeypatch.setattr("dags.loading_in_datalike_minio.PostgresHook", mock_hook_class)

    return mock_hook, mock_cursor


@pytest.fixture
def mock_s3fs(monkeypatch):
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    mock_fs.mkdir.return_value = None
    mock_fs.open.return_value.__enter__.return_value = MagicMock()
    mock_fs.open.return_value.write = MagicMock()
    mock_fs.open.return_value.close = MagicMock()
    mock_fs.ls.return_value = []
    mock_fs.rm = MagicMock()
    mock_fs.info.return_value = {"size": 1024}

    def fake_get_s3fs(*args, **kwargs):
        return mock_fs

    monkeypatch.setattr(
        "dags.loading_in_datalike_minio.get_s3_filesystem", fake_get_s3fs
    )
    return mock_fs


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("MINIO_DEFAULT_BUCKET", "test-bucket")


@pytest.fixture
def mock_dag_run():
    dag_run = MagicMock()
    dag_run.run_id = "test_run_id"
    dag_run.conf = {}
    return dag_run


@pytest.fixture
def context(mock_dag_run):
    return {
        "dag_run": mock_dag_run,
        "ds": "2025-10-01",
        "next_ds": "2025-10-02",
    }


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


def test_dag_structure():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags.get("postgres_to_minio_enterprise")
    assert dag is not None
    assert "@daily" in str(dag.timetable)
    assert dag.catchup is True

    task_ids = [t.task_id for t in dag.tasks]
    expected_tables = [f"extract_load_{tbl}" for tbl in TABLES_CONFIG.keys()]
    for tbl_task in expected_tables:
        assert tbl_task in task_ids
    assert "trigger_silver_to_gold" in task_ids


def test_acquire_partition_lock_insert_new(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    mock_cursor.fetchone.side_effect = [("processing",), None]

    result = acquire_partition_lock("wells", "2025-10-01", "run1")
    assert result is True
    assert mock_cursor.execute.call_count >= 1


def test_acquire_partition_lock_already_loaded(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    mock_cursor.fetchone.side_effect = [None, ("loaded", datetime.now(timezone.utc))]

    result = acquire_partition_lock("wells", "2025-10-01", "run2")
    assert result is False


def test_acquire_partition_lock_stale_lock(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=40)
    mock_cursor.fetchone.side_effect = [None, ("processing", stale_time)]

    result = acquire_partition_lock("wells", "2025-10-01", "run3")
    assert result is True


def test_acquire_partition_lock_active_lock(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    fresh_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    mock_cursor.fetchone.side_effect = [None, ("processing", fresh_time)]

    with pytest.raises(RuntimeError, match="locked by another run"):
        acquire_partition_lock("wells", "2025-10-01", "run4")


def test_release_partition_lock_success(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    release_partition_lock(
        "wells", "2025-10-01", success=True, file_path="s3://path", row_count=100
    )

    assert mock_cursor.execute.call_count == 1
    update_call = mock_cursor.execute.call_args_list[0]
    assert "UPDATE" in update_call[0][0]


def test_release_partition_lock_failure(mock_postgres_hook):
    mock_hook, mock_cursor = mock_postgres_hook
    release_partition_lock("wells", "2025-10-01", success=False)

    assert mock_cursor.execute.call_count == 1
    update_call = mock_cursor.execute.call_args_list[0]
    assert "status = 'failed'" in update_call[0][0]


def test_cast_batch_to_schema_missing_fields():
    source_schema = pa.schema([("a", pa.int32()), ("c", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array(["x", "y"])], schema=source_schema
    )
    target_schema = pa.schema(
        [("a", pa.int32()), ("b", pa.float64()), ("c", pa.string())]
    )

    result = _cast_batch_to_schema(batch, target_schema)
    assert result.schema == target_schema


def test_cast_batch_to_schema_decimal_to_float64():
    source_schema = pa.schema([("amount", pa.decimal128(10, 2))])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([Decimal("123.45"), Decimal("67.89")])], schema=source_schema
    )
    target_schema = pa.schema([("amount", pa.float64())])

    result = _cast_batch_to_schema(batch, target_schema)
    assert result.schema == target_schema
    assert result.column("amount").to_pylist() == [123.45, 67.89]


def test_cast_timestamp_to_microseconds():
    source_schema = pa.schema([("ts", pa.timestamp("s"))])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([datetime(2025, 10, 1, 12, 30, 45)])], schema=source_schema
    )
    target_schema = pa.schema([("ts", pa.timestamp("us"))])

    result = _cast_batch_to_schema(batch, target_schema)

    assert result.schema.field("ts").type == pa.timestamp("us")
    # Проверяем, что микросекунды не потерялись
    assert result.column("ts")[0].as_py() == datetime(2025, 10, 1, 12, 30, 45)


def test_extract_load_fact_table_success(
    mock_postgres_hook, mock_s3fs, mock_env, context
):
    mock_hook, mock_cursor = mock_postgres_hook

    mock_cursor.fetchmany.side_effect = [
        [(1, "Well1", "Field1", "North", date(2025, 10, 1), "OperatorA", "active")],
        [],
    ]

    with (
        patch("dags.loading_in_datalike_minio.pq.ParquetWriter") as MockWriter,
        patch("dags.loading_in_datalike_minio.pq.ParquetFile") as MockPQ,
    ):

        mock_writer = MagicMock()
        MockWriter.return_value = mock_writer

        mock_parquet_file = MagicMock()
        mock_parquet_file.metadata.num_rows = 1
        MockPQ.return_value = mock_parquet_file

        cfg = TABLES_CONFIG["wells"]
        extract_load("wells", cfg, **context)

        mock_writer.write_batch.assert_called()


def test_extract_load_fact_table_zero_rows(
    mock_postgres_hook, mock_s3fs, mock_env, context
):
    mock_hook, mock_cursor = mock_postgres_hook
    mock_cursor.fetchmany.return_value = []

    cfg = TABLES_CONFIG["wells"]
    extract_load("wells", cfg, **context)

    mock_s3fs.open.assert_not_called()


def test_extract_load_non_fact_table_success(
    mock_postgres_hook, mock_s3fs, mock_env, context
):
    mock_hook, mock_cursor = mock_postgres_hook
    mock_s3fs.exists.return_value = False

    mock_cursor.fetchmany.side_effect = [
        [(1, "Well1", "Field1", "North", date(2025, 10, 1), "OperatorA", "active")],
        [],
    ]

    with (
        patch("dags.loading_in_datalike_minio.pq.ParquetWriter") as MockWriter,
        patch("dags.loading_in_datalike_minio.pq.ParquetFile") as MockPQ,
    ):
        mock_writer = MagicMock()
        MockWriter.return_value = mock_writer
        mock_parquet_file = MagicMock()
        mock_parquet_file.metadata.num_rows = 1
        MockPQ.return_value = mock_parquet_file

        cfg = TABLES_CONFIG["wells"]
        extract_load("wells", cfg, **context)

        mock_s3fs.mkdir.assert_called_once()
        mock_writer.write_batch.assert_called()


def test_extract_load_integrity_check_failure(
    mock_postgres_hook, mock_s3fs, mock_env, context
):
    mock_hook, mock_cursor = mock_postgres_hook

    mock_cursor.fetchmany.side_effect = [
        [(1, "Well1", "Field1", "North", date(2025, 10, 1), "OperatorA", "active")],
        [],
    ]

    with (
        patch("dags.loading_in_datalike_minio.pq.ParquetWriter") as MockWriter,
        patch("dags.loading_in_datalike_minio.pq.ParquetFile") as MockPQ,
    ):
        mock_writer = MagicMock()
        MockWriter.return_value = mock_writer
        mock_parquet_file = MagicMock()
        mock_parquet_file.metadata.num_rows = 999
        MockPQ.return_value = mock_parquet_file

        # Для удаления файла нужно, чтобы exists() для .parquet вернул True
        mock_s3fs.exists.side_effect = lambda path: path.endswith(".parquet")

        cfg = TABLES_CONFIG["wells"]
        with pytest.raises(ValueError, match="Integrity check failed"):
            extract_load("wells", cfg, **context)

    mock_s3fs.rm.assert_called_once()


def test_extract_load_error_during_processing(
    mock_postgres_hook, mock_s3fs, mock_env, context
):
    mock_hook, mock_cursor = mock_postgres_hook
    mock_cursor.fetchmany.side_effect = Exception("DB error")

    cfg = TABLES_CONFIG["production"]  # fact-таблица, блокировка будет взята
    with pytest.raises(Exception, match="DB error"):
        extract_load("production", cfg, **context)

    # Должен быть вызов UPDATE ... SET status = 'failed'
    update_calls = [
        c for c in mock_cursor.execute.call_args_list if "status = 'failed'" in c[0][0]
    ]
    assert len(update_calls) == 1, "Expected one release of lock with status='failed'"


def test_extract_load_range_of_dates(mock_postgres_hook, mock_s3fs, mock_env, context):

    context["dag_run"].conf = {"start_date": "2025-10-01", "end_date": "2025-10-03"}

    mock_hook, mock_cursor = mock_postgres_hook
    mock_cursor.fetchmany.side_effect = [
        [(1, 100, date(2025, 10, 1), 10.0, 5.0, 2.0, 100.0, 0.5, 90.0, 14.7)],
        [],
        [(2, 100, date(2025, 10, 2), 12.0, 6.0, 3.0, 110.0, 0.0, 92.0, 15.0)],
        [],
        [(3, 100, date(2025, 10, 3), 11.0, 4.0, 1.0, 105.0, 1.0, 88.0, 13.5)],
        [],
    ]

    with (
        patch("dags.loading_in_datalike_minio.pq.ParquetWriter") as MockWriter,
        patch("dags.loading_in_datalike_minio.pq.ParquetFile") as MockPQ,
    ):
        mock_writer = MagicMock()
        MockWriter.return_value = mock_writer
        mock_parquet_file = MagicMock()
        mock_parquet_file.metadata.num_rows = 1
        MockPQ.return_value = mock_parquet_file

        cfg = TABLES_CONFIG["production"]
        extract_load("production", cfg, **context)

        assert mock_writer.write_batch.call_count == 3
