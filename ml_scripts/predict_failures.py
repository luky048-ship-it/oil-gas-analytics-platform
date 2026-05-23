import argparse
import logging
import pickle
from datetime import datetime

import polars as pl
from db_utils import (get_postgres_uri, get_psycopg2_conn,
                      get_s3_storage_options)

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


def predict(target_date_str: str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    storage_opts = get_s3_storage_options()

    sensors_today = load_silver_partition("pump_sensors", target_date_str, storage_opts)
    if sensors_today.height == 0:
        logger.info("Нет данных pump_sensors на %s", target_date_str)
        return

    # Присоединение отказов
    try:
        failures_today = load_silver_partition(
            "pump_failures", target_date_str, storage_opts
        )
    except Exception:
        failures_today = pl.DataFrame(
            schema=["pump_id", "failure_date", "failure_type"]
        )
    if failures_today.height > 0:
        sensors_today = sensors_today.join(
            failures_today.select(["pump_id", "failure_date", "failure_type"]),
            left_on=["pump_id", "timestamp"],
            right_on=["pump_id", "failure_date"],
            how="left",
        ).with_columns(
            pl.col("failure_type").is_not_null().cast(pl.Boolean).alias("is_failure")
        )
    else:
        sensors_today = sensors_today.with_columns(pl.lit(False).alias("is_failure"))

    features = ["temperature", "vibration", "current", "rpm", "pressure"]
    sensors_today = sensors_today.with_columns(
        [pl.col(f).fill_null(pl.col(f).mean().over("pump_id")) for f in features]
    ).drop_nulls(subset=["pump_id"])

    X_today = sensors_today.select(features).to_numpy()

    # Загрузка моделей
    with open(MODEL_ISO_PATH, "rb") as f:
        iso = pickle.load(f)
    with open(MODEL_RF_PATH, "rb") as f:
        rf = pickle.load(f)

    is_anomaly_ml = [bool(x == -1) for x in iso.predict(X_today)]

    if rf is None:
        prob_failure = [0.0] * len(X_today)
    else:
        prob_failure = rf.predict_proba(X_today)[:, 1]

    result = sensors_today.select(["record_id", "pump_id", "timestamp"]).with_columns(
        [
            pl.Series("is_anomaly_ml", is_anomaly_ml),
            pl.Series("risk_score", prob_failure),
            pl.Series("failure_probability", prob_failure),
            pl.lit("PumpClassifier_v1.0").alias("model_version"),
            pl.lit(datetime.now()).alias("scored_at"),
        ]
    )

    uri = get_postgres_uri()
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM gold.ml_pump_predictions WHERE timestamp::date = '{target_date_str}'"
            )
        conn.commit()
    finally:
        conn.close()

    result.write_database(
        table_name="gold.ml_pump_predictions",
        connection=uri,
        if_table_exists="append",
        engine="adbc",
    )
    logger.info("Записано %d строк результатов анализа насосов", result.height)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date", help="одна дата (необязательно, если есть TARGET_DATES_JSON)"
    )
    args = parser.parse_args()

    dates_json = os.environ.get("TARGET_DATES_JSON")
    if dates_json:
        target_dates = json.loads(dates_json)
    elif args.target_date:
        target_dates = [args.target_date]
    else:
        raise RuntimeError("Укажите --target-date или переменную TARGET_DATES_JSON")

    errors = []
    for date_str in target_dates:
        try:
            run_flow_prediction(date_str)
        except Exception as e:
            logging.error("Ошибка обработки даты %s: %s", date_str, e)
            errors.append(date_str)

    if errors:
        raise RuntimeError(f"Не удалось обработать даты: {errors}")


if __name__ == "__main__":
    main()
