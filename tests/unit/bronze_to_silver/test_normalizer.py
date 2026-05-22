from datetime import datetime

import polars as pl

from plugins.bronze_to_silver.normalizer import normalize_dataset


def test_casts_columns_to_expected_types():
    df = pl.DataFrame(
        {
            "well_id": ["1", "2"],
            "production": ["100.5", "200.7"],
        }
    )

    schema_contract = {
        "columns": {
            "well_id": pl.Int64,
            "production": pl.Float64,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="production",
        schema_contract=schema_contract,
    ).collect()

    assert result.schema["well_id"] == pl.Int64
    assert result.schema["production"] == pl.Float64

    assert result["well_id"].to_list() == [1, 2]
    assert result["production"].to_list() == [100.5, 200.7]


def test_truncates_datetime_to_seconds():
    df = pl.DataFrame(
        {
            "event_time": [
                datetime(2025, 1, 1, 12, 30, 45, 123456),
            ]
        }
    )

    schema_contract = {
        "columns": {
            "event_time": pl.Datetime("us"),
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="production",
        schema_contract=schema_contract,
    ).collect()

    value = result["event_time"][0]

    assert value.microsecond == 0


def test_replaces_nan_and_inf_with_null():
    df = pl.DataFrame(
        {
            "pressure": [100.0, float("nan"), float("inf")],
        }
    )

    schema_contract = {
        "columns": {
            "pressure": pl.Float64,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="production",
        schema_contract=schema_contract,
    ).collect()

    assert result["pressure"].to_list() == [100.0, None, None]


def test_strips_whitespace_from_strings():
    df = pl.DataFrame(
        {
            "operator_name": [
                "  Shell  ",
                "\tBP\n",
            ]
        }
    )

    schema_contract = {
        "columns": {
            "operator_name": pl.String,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="operators",
        schema_contract=schema_contract,
    ).collect()

    assert result["operator_name"].to_list() == [
        "Shell",
        "BP",
    ]


def test_adds_technical_processing_column():
    df = pl.DataFrame(
        {
            "well_id": [1],
        }
    )

    schema_contract = {
        "columns": {
            "well_id": pl.Int64,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="wells",
        schema_contract=schema_contract,
    ).collect()

    assert "_silver_processed_at" in result.columns

    processed_at = result["_silver_processed_at"][0]

    assert processed_at is not None
    assert processed_at.tzinfo is not None


def test_keeps_only_columns_from_schema_contract():
    df = pl.DataFrame(
        {
            "well_id": [1],
            "production": [100.0],
            "unexpected_column": ["SHOULD_BE_REMOVED"],
        }
    )

    schema_contract = {
        "columns": {
            "well_id": pl.Int64,
            "production": pl.Float64,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="production",
        schema_contract=schema_contract,
    ).collect()

    assert result.columns == [
        "well_id",
        "production",
        "_silver_processed_at",
    ]


def test_preserves_null_values():
    df = pl.DataFrame(
        {
            "production": [100.0, None],
        }
    )

    schema_contract = {
        "columns": {
            "production": pl.Float64,
        }
    }

    result = normalize_dataset(
        lf=df.lazy(),
        dataset="production",
        schema_contract=schema_contract,
    ).collect()

    assert result["production"].to_list() == [100.0, None]
