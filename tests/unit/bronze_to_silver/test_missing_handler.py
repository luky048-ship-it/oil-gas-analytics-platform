import polars as pl

from plugins.bronze_to_silver.missing_handler import handle_missing_values


def test_returns_original_lazyframe_when_no_rules():
    df = pl.DataFrame(
        {
            "pressure": [100.0, None, 120.0],
        }
    )

    result = handle_missing_values(
        lf=df.lazy(),
        missing_rules={},
    ).collect()

    assert result.equals(df)


def test_fills_nulls_with_static_value():
    df = pl.DataFrame(
        {
            "pressure": [100.0, None, 120.0],
        }
    )

    missing_rules = {
        "pressure": {
            "strategy": "fill_value",
            "value": 0.0,
        }
    }

    result = handle_missing_values(
        lf=df.lazy(),
        missing_rules=missing_rules,
    ).collect()

    assert result["pressure"].to_list() == [
        100.0,
        0.0,
        120.0,
    ]


def test_forward_fill_within_partition():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 1, 2, 2],
            "event_time": [1, 2, 3, 1, 2],
            "pressure": [100.0, None, 120.0, 200.0, None],
        }
    )

    missing_rules = {
        "pressure": {
            "strategy": "forward_fill",
            "partition_by": "well_id",
            "order_by": "event_time",
        }
    }

    result = (
        handle_missing_values(
            lf=df.lazy(),
            missing_rules=missing_rules,
        )
        .collect()
        .sort(["well_id", "event_time"])
    )

    assert result["pressure"].to_list() == [
        100.0,
        100.0,
        120.0,
        200.0,
        200.0,
    ]


def test_forward_fill_does_not_cross_partitions():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 2],
            "event_time": [1, 2, 1],
            "pressure": [100.0, None, None],
        }
    )

    missing_rules = {
        "pressure": {
            "strategy": "forward_fill",
            "partition_by": "well_id",
            "order_by": "event_time",
        }
    }

    result = (
        handle_missing_values(
            lf=df.lazy(),
            missing_rules=missing_rules,
        )
        .collect()
        .sort(["well_id", "event_time"])
    )

    assert result["pressure"].to_list() == [
        100.0,
        100.0,
        None,
    ]


def test_handles_multiple_missing_strategies():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 2],
            "event_time": [1, 2, 1],
            "pressure": [100.0, None, None],
            "temperature": [None, 80.0, None],
        }
    )

    missing_rules = {
        "pressure": {
            "strategy": "forward_fill",
            "partition_by": "well_id",
            "order_by": "event_time",
        },
        "temperature": {
            "strategy": "fill_value",
            "value": 0.0,
        },
    }

    result = (
        handle_missing_values(
            lf=df.lazy(),
            missing_rules=missing_rules,
        )
        .collect()
        .sort(["well_id", "event_time"])
    )

    assert result["pressure"].to_list() == [
        100.0,
        100.0,
        None,
    ]

    assert result["temperature"].to_list() == [
        0.0,
        80.0,
        0.0,
    ]


def test_sorts_before_forward_fill():
    df = pl.DataFrame(
        {
            "well_id": [1, 1, 1],
            "event_time": [3, 1, 2],
            "pressure": [120.0, 100.0, None],
        }
    )

    missing_rules = {
        "pressure": {
            "strategy": "forward_fill",
            "partition_by": "well_id",
            "order_by": "event_time",
        }
    }

    result = (
        handle_missing_values(
            lf=df.lazy(),
            missing_rules=missing_rules,
        )
        .collect()
        .sort("event_time")
    )

    assert result["pressure"].to_list() == [
        100.0,
        100.0,
        120.0,
    ]


def test_preserves_schema_after_missing_handling():
    df = pl.DataFrame(
        {
            "well_id": [1],
            "pressure": [None],
        },
        {"well_id": pl.Int32, "pressure": pl.Float64},
    )

    missing_rules = {
        "pressure": {
            "strategy": "fill_value",
            "value": 0.0,
        }
    }

    result = handle_missing_values(
        lf=df.lazy(),
        missing_rules=missing_rules,
    ).collect()

    assert result.schema == df.schema
