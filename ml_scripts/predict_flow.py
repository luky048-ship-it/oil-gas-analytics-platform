# plugins/ml_scripts/predict_flow.py
import argparse
import logging
import pickle
from datetime import datetime

import polars as pl
from db_utils import get_postgres_uri, get_psycopg2_conn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_PATH = "/opt/airflow/ml_models/flow_model.pkl"


def predict(target_date_str: str):
    uri = get_postgres_uri()
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)

    query = """
        SELECT well_id, date, oil_ton,
               avg_temperature, avg_pressure, energy_kwh, downtime_hours
        FROM gold.mart_production
        WHERE date = %(date)s
    """
    df = pl.read_database_uri(
        query,
        uri,
        engine="adbc",
        execute_options={"parameters": {"date": target_date_str}},
    )
    if df.height == 0:
        logger.info("Нет данных за %s", target_date_str)
        return

    features = ["avg_temperature", "avg_pressure", "energy_kwh", "downtime_hours"]
    X = df.select(features).to_numpy()
    preds = model.predict(X)

    result = df.select(["well_id", "date", "oil_ton"]).rename(
        {"oil_ton": "actual_oil_ton"}
    )
    result = result.with_columns(
        [
            pl.Series("predicted_oil_ton", preds),
            pl.lit("RandomForest_v1.0").alias("model_version"),
            pl.lit(datetime.now()).alias("scored_at"),
        ]
    )
    result = result.with_columns(
        (pl.col("actual_oil_ton") - pl.col("predicted_oil_ton")).alias(
            "prediction_error"
        )
    )

    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM gold.ml_flow_predictions WHERE date = '{target_date_str}'"
            )
        conn.commit()
    finally:
        conn.close()

    result.write_database(
        table_name="gold.ml_flow_predictions",
        connection=uri,
        if_table_exists="append",
        engine="adbc",
    )
    logger.info("Записано %d прогнозов дебита", result.height)


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
