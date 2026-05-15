# plugins/dq_utils/statistical_validator.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Tuple

import polars as pl

from dq_utils.dq_reporter import DQResult

logger = logging.getLogger(__name__)


def validate_volume_anomaly(
    dataset: str, current_count: int, historical_avg: float, threshold_std: float = 3.0
) -> DQResult:
    """
    Validates dataset row count against historical averages to detect sudden drops or spikes.
    Non-critical validation (WARNING level).
    """
    if historical_avg <= 0:
        return DQResult(
            dataset,
            "Volume Anomaly",
            "PASS",
            0,
            current_count,
            "No historical data.",
            datetime.utcnow(),
        )

    # Simplified z-score approach assuming Poisson-like variance where std ~ sqrt(mean)
    # In a real scenario, historical standard deviation should be fetched from etl_metadata
    estimated_std = (historical_avg**0.5) if historical_avg > 0 else 1.0
    z_score = abs(current_count - historical_avg) / estimated_std

    status = "WARNING" if z_score > threshold_std else "PASS"
    msg = f"Volume z-score: {z_score:.2f}. Current: {current_count}, HistAvg: {historical_avg:.1f}."

    if status == "WARNING":
        logger.warning(f"Volume anomaly detected for '{dataset}': {msg}")

    return DQResult(
        dataset=dataset,
        validation_type="Volume Anomaly",
        status=status,
        failed_rows=0,
        checked_rows=current_count,
        message=msg,
        created_at=datetime.utcnow(),
    )


def validate_distribution_drift(
    lf: pl.LazyFrame,
    dataset: str,
    monitored_columns: List[str],
    historical_stats: Dict[str, Tuple[float, float]],
) -> List[DQResult]:
    """
    Calculates mean and standard deviation for monitored columns lazily.
    Compares against historical stats to detect statistical drift (e.g., sensor calibration issues).
    """
    if not monitored_columns or not historical_stats:
        return []

    agg_exprs = []
    for col in monitored_columns:
        if col in historical_stats:
            agg_exprs.extend(
                [
                    pl.col(col).mean().alias(f"{col}_mean"),
                    pl.col(col).std().alias(f"{col}_std"),
                ]
            )

    if not agg_exprs:
        return []

    stats_df = lf.select(agg_exprs).collect()
    total_rows = lf.select(pl.len()).collect().item()

    results = []
    for col in monitored_columns:
        if col not in historical_stats:
            continue

        hist_mean, hist_std = historical_stats[col]
        curr_mean = stats_df[f"{col}_mean"][0]

        if curr_mean is None or hist_std == 0:
            continue

        # Calculate drift z-score
        drift_z = abs(curr_mean - hist_mean) / hist_std

        status = "WARNING" if drift_z > 3.0 else "PASS"
        msg = f"Drift z-score: {drift_z:.2f} (CurrMean: {curr_mean:.2f}, HistMean: {hist_mean:.2f})"

        if status == "WARNING":
            logger.warning(
                f"Statistical drift detected in '{dataset}' column '{col}': {msg}"
            )

        results.append(
            DQResult(
                dataset=dataset,
                validation_type=f"Distribution Drift: {col}",
                status=status,
                failed_rows=0,
                checked_rows=total_rows,
                message=msg,
                created_at=datetime.utcnow(),
            )
        )

    return results
