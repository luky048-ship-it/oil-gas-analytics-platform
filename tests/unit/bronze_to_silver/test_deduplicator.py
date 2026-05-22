import polars as pl

from plugins.bronze_to_silver.deduplicator import deduplicate_dataset


def test_returns_same_lazyframe_when_no_key_columns():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 2],
            "production": [100, 100, 200],
        }
    )

    lf = df.lazy()

    result = deduplicate_dataset(
        lf=lf,
        key_columns=[],
        timestamp_column=None,
    ).collect()

    assert result.equals(df)


def test_removes_duplicates_without_timestamp():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 2],
            "production": [100, 100, 200],
        }
    )

    result = (
        deduplicate_dataset(
            lf=df.lazy(),
            key_columns=["well_id"],
            timestamp_column=None,
        )
        .collect()
        .sort("well_id")
    )

    expected = pl.DataFrame(
        {
            "well_id": [1, 2],
            "production": [100, 200],
        }
    )

    assert result.equals(expected)


def test_keeps_latest_record_by_timestamp():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 1],
            "production": [100, 150, 200],
            "event_time": [
                "2025-01-01 10:00:00",
                "2025-01-01 12:00:00",
                "2025-01-01 14:00:00",
            ],
        }
    ).with_columns(pl.col("event_time").str.strptime(pl.Datetime))

    result = deduplicate_dataset(
        lf=df.lazy(),
        key_columns=["well_id"],
        timestamp_column="event_time",
    ).collect()

    assert result.height == 1
    assert result["production"][0] == 200


def test_deduplicates_by_multiple_keys():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 1],
            "field_id": [10, 10, 20],
            "production": [100, 150, 300],
        }
    )

    result = (
        deduplicate_dataset(
            lf=df.lazy(),
            key_columns=["well_id", "field_id"],
            timestamp_column=None,
        )
        .collect()
        .sort(["well_id", "field_id"])
    )

    expected = pl.DataFrame(
        {
            "well_id": [1, 1],
            "field_id": [10, 20],
            "production": [150, 300],
        }
    )

    assert result.equals(expected)


def test_keeps_last_record_when_duplicates_exist():
    df = pl.DataFrame(
        {
            "well_id": [1, 1],
            "production": [100, 999],
        }
    )

    result = deduplicate_dataset(
        lf=df.lazy(),
        key_columns=["well_id"],
        timestamp_column=None,
    ).collect()

    assert result.height == 1
    assert result["production"][0] == 999
