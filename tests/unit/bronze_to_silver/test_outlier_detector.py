import polars as pl

from plugins.bronze_to_silver.outlier_detector import (
    detect_outliers,
)


def test_returns_original_lazyframe_when_no_monitored_columns():
    df = pl.DataFrame(
        {
            "pressure": [100.0, 110.0, 120.0],
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=[],
    )

    valid_df = valid_lf.collect()

    assert valid_df.equals(df)
    assert invalid_lf is None


def test_detects_outliers_using_iqr():
    df = pl.DataFrame(
        {
            "pressure": [
                100.0,
                105.0,
                110.0,
                115.0,
                5000.0,
            ]
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=["pressure"],
        multiplier=1.5,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df["pressure"].to_list() == [
        100.0,
        105.0,
        110.0,
        115.0,
    ]

    assert invalid_df["pressure"].to_list() == [
        5000.0,
    ]


def test_returns_empty_invalid_when_no_outliers():
    df = pl.DataFrame(
        {
            "pressure": [
                100.0,
                105.0,
                110.0,
                115.0,
            ]
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=["pressure"],
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 4
    assert invalid_df.height == 0


def test_detects_outliers_in_multiple_columns():
    df = pl.DataFrame(
        {
            "pressure": [
                100.0,
                110.0,
                120.0,
                10000.0,
            ],
            "temperature": [
                80.0,
                82.0,
                85.0,
                90.0,
            ],
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=[
            "pressure",
            "temperature",
        ],
        multiplier=1.5,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 3
    assert invalid_df.height == 1

    assert invalid_df["pressure"][0] == 10000.0


def test_marks_row_invalid_if_any_column_is_outlier():
    df = pl.DataFrame(
        {
            "pressure": [
                100.0,
                110.0,
                120.0,
                130.0,
            ],
            "temperature": [
                80.0,
                82.0,
                85.0,
                9999.0,
            ],
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=[
            "pressure",
            "temperature",
        ],
        multiplier=1.5,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.height == 3
    assert invalid_df.height == 1

    assert invalid_df["temperature"][0] == 9999.0


def test_respects_custom_multiplier():
    df = pl.DataFrame(
        {
            "pressure": [
                100.0,
                105.0,
                110.0,
                150.0,
            ]
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=["pressure"],
        multiplier=0.5,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert invalid_df.height >= 1


def test_preserves_schema_after_filtering():
    df = pl.DataFrame(
        {
            "well_id": [1, 2, 3, 4],
            "pressure": [100.0, 105.0, 110.0, 9999.0],
        }
    )

    valid_lf, invalid_lf = detect_outliers(
        lf=df.lazy(),
        monitored_columns=["pressure"],
        multiplier=1.5,
    )

    valid_df = valid_lf.collect()
    invalid_df = invalid_lf.collect()

    assert valid_df.schema == df.schema
    assert invalid_df.schema == df.schema
