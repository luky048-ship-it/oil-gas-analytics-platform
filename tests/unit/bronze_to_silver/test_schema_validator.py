import logging

import polars as pl

from plugins.bronze_to_silver.schema_validator import (filter_by_data_quality,
                                                       validate_dataset_schema)

# =========================================================
# validate_dataset_schema
# =========================================================


def test_returns_valid_lazyframe_when_schema_is_correct():
    df = pl.DataFrame(
        {
            "well_id": [1, 2],
            "production": [100.0, 200.0],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    valid_lf, invalid_lf = validate_dataset_schema(
        lf=df.lazy(),
        dataset="production",
        expected_schema=expected_schema,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 2
    assert invalid_df.height == 0


def test_returns_all_rows_as_invalid_when_column_missing():
    df = pl.DataFrame(
        {
            "well_id": [1, 2],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    valid_lf, invalid_lf = validate_dataset_schema(
        lf=df.lazy(),
        dataset="production",
        expected_schema=expected_schema,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 0
    assert invalid_df.height == 2


def test_returns_all_rows_as_invalid_when_types_mismatch():
    df = pl.DataFrame(
        {
            "well_id": ["1", "2"],
            "production": [100.0, 200.0],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    valid_lf, invalid_lf = validate_dataset_schema(
        lf=df.lazy(),
        dataset="production",
        expected_schema=expected_schema,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 0
    assert invalid_df.height == 2


def test_logs_warning_when_schema_invalid(caplog):
    df = pl.DataFrame(
        {
            "well_id": ["1"],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    with caplog.at_level(logging.WARNING):
        validate_dataset_schema(
            lf=df.lazy(),
            dataset="production",
            expected_schema=expected_schema,
        )

    assert "Schema validation failed" in caplog.text
    assert "All rows will be sent to quarantine" in caplog.text


def test_logs_warning_for_new_columns(caplog):
    df = pl.DataFrame(
        {
            "well_id": [1],
            "production": [100.0],
            "unexpected_column": ["NEW"],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    with caplog.at_level(logging.WARNING):
        validate_dataset_schema(
            lf=df.lazy(),
            dataset="production",
            expected_schema=expected_schema,
        )

    assert "new unexpected columns" in caplog.text
    assert "unexpected_column" in caplog.text


def test_keeps_valid_rows_when_extra_columns_exist():
    df = pl.DataFrame(
        {
            "well_id": [1],
            "production": [100.0],
            "extra_metadata": ["abc"],
        }
    )

    expected_schema = {
        "well_id": pl.Int64,
        "production": pl.Float64,
    }

    valid_lf, invalid_lf = validate_dataset_schema(
        lf=df.lazy(),
        dataset="production",
        expected_schema=expected_schema,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 1
    assert invalid_df.height == 0


# =========================================================
# filter_by_data_quality
# =========================================================


def test_returns_all_valid_when_no_validation_rules():
    df = pl.DataFrame(
        {
            "pressure": [100.0, 120.0],
        }
    )

    valid_lf, invalid_lf = filter_by_data_quality(
        lf=df.lazy(),
        validation_rules={},
    )

    assert valid_lf.collect().height == 2
    assert invalid_lf.collect().height == 0


def test_filters_invalid_enum_values():
    df = pl.DataFrame(
        {
            "status": ["active", "invalid", "inactive"],
        }
    )

    validation_rules = {
        "enums": {
            "status": ["active", "inactive"],
        }
    }

    valid_lf, invalid_lf = filter_by_data_quality(
        lf=df.lazy(),
        validation_rules=validation_rules,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df["status"].to_list() == [
        "active",
        "inactive",
    ]

    assert invalid_df["status"].to_list() == [
        "invalid",
    ]


def test_filters_invalid_range_values():
    df = pl.DataFrame(
        {
            "pressure": [50.0, 150.0, 300.0],
        }
    )

    validation_rules = {
        "ranges": {
            "pressure": {
                "min": 100.0,
                "max": 200.0,
            }
        }
    }

    valid_lf, invalid_lf = filter_by_data_quality(
        lf=df.lazy(),
        validation_rules=validation_rules,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df["pressure"].to_list() == [150.0]
    assert invalid_df["pressure"].to_list() == [50.0, 300.0]


def test_filters_invalid_custom_rule():
    df = pl.DataFrame(
        {
            "oil_volume": [100.0, None],
        }
    )

    validation_rules = {
        "custom": [
            {
                "rule": "oil_volume is not null",
            }
        ]
    }

    valid_lf, invalid_lf = filter_by_data_quality(
        lf=df.lazy(),
        validation_rules=validation_rules,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 1
    assert invalid_df.height == 1


def test_combines_multiple_validation_rules():
    df = pl.DataFrame(
        {
            "status": ["active", "bad"],
            "pressure": [150.0, 500.0],
        }
    )

    validation_rules = {
        "enums": {
            "status": ["active"],
        },
        "ranges": {
            "pressure": {
                "max": 300.0,
            }
        },
    }

    valid_lf, invalid_lf = filter_by_data_quality(
        lf=df.lazy(),
        validation_rules=validation_rules,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 1
    assert invalid_df.height == 1


def test_logs_warning_when_custom_rule_invalid(caplog):
    df = pl.DataFrame(
        {
            "pressure": [100.0],
        }
    )

    validation_rules = {
        "custom": [
            {
                "rule": "THIS IS INVALID SQL",
            }
        ]
    }

    with caplog.at_level(logging.WARNING):
        filter_by_data_quality(
            lf=df.lazy(),
            validation_rules=validation_rules,
        )

    assert "Failed to parse custom DQ rule" in caplog.text
