import argparse
import logging
import pickle
from datetime import datetime, timedelta

import polars as pl
from db_utils import get_s3_storage_options
from sklearn.ensemble import IsolationForest, RandomForestClassifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_BASE = "s3://datalake/silver"
MODEL_ISO_PATH = "/opt/airflow/ml_models/iso_forest.pkl"
MODEL_RF_PATH = "/opt/airflow/ml_models/rf_pump.pkl"


def load_silver_partition(
    table: str, partition_date: str, storage_options: dict
) -> pl.DataFrame:
    path = f"{SILVER_BASE}/{table}/partition_date={partition_date}/*.parquet"
    return pl.read_parquet(path, storage_options=storage_options)


def train(days_back: int = 90):
    storage_opts = get_s3_storage_options()
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)

    sensors_list, failures_list = [], []
    current = start_date
    while current <= end_date:
        dt_str = current.isoformat()
        try:
            s = load_silver_partition("pump_sensors", dt_str, storage_opts)
            if s.height > 0:
                sensors_list.append(s)
            f = load_silver_partition("pump_failures", dt_str, storage_opts)
            if f.height > 0:
                failures_list.append(f)
        except Exception:
            pass
        current += timedelta(days=1)

    if not sensors_list:
        raise RuntimeError("Нет исторических данных для обучения")

    sensors_hist = pl.concat(sensors_list)
    failures_hist = (
        pl.concat(failures_list)
        if failures_list
        else pl.DataFrame(schema=["pump_id", "failure_date", "failure_type"])
    )

    # Объединение сенсоров с отказами
    if failures_hist.height > 0:
        sensors_hist = sensors_hist.join(
            failures_hist.select(["pump_id", "failure_date", "failure_type"]),
            left_on=["pump_id", "timestamp"],
            right_on=["pump_id", "failure_date"],
            how="left",
        ).with_columns(
            pl.col("failure_type").is_not_null().cast(pl.Boolean).alias("is_failure")
        )
    else:
        sensors_hist = sensors_hist.with_columns(pl.lit(False).alias("is_failure"))

    features = ["temperature", "vibration", "current", "rpm", "pressure"]
    # Заполнение пропусков средними по pump_id
    sensors_hist = sensors_hist.with_columns(
        [pl.col(f).fill_null(pl.col(f).mean().over("pump_id")) for f in features]
    ).drop_nulls(subset=["pump_id"])

    X = sensors_hist.select(features).to_numpy()

    # Isolation Forest
    iso = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
    iso.fit(X)
    with open(MODEL_ISO_PATH, "wb") as f:
        pickle.dump(iso, f)
    logger.info("IsolationForest сохранён")

    # Random Forest Classifier
    y = sensors_hist.select("is_failure").to_numpy().ravel().astype(int)
    if y.sum() == 0:
        logger.warning("Нет отказов, RF не обучен. Будет использоваться заглушка.")
        rf = None
    else:
        rf = RandomForestClassifier(
            n_estimators=100, max_depth=5, random_state=42, n_jobs=-1
        )
        rf.fit(X, y)
    with open(MODEL_RF_PATH, "wb") as f:
        pickle.dump(rf, f)
    logger.info("RandomForest сохранён")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=90)
    args = parser.parse_args()
    train(args.days_back)
