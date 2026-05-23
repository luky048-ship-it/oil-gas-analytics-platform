import argparse
import logging
import pickle
from datetime import datetime, timedelta

import polars as pl
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

from db_utils import get_postgres_uri

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_PATH = "/opt/airflow/ml_models/flow_model.pkl"


def train(days_back: int = 365):
    uri = get_postgres_uri()
    cutoff = (datetime.now().date() - timedelta(days=1)).isoformat()
    start = (datetime.now().date() - timedelta(days=days_back + 1)).isoformat()

    query = """
        SELECT well_id, date, oil_ton,
               avg_temperature, avg_pressure, energy_kwh, downtime_hours
        FROM gold.mart_production
        WHERE date BETWEEN %(start)s AND %(end)s
    """
    df = pl.read_database_uri(
        query,
        uri,
        engine="adbc",
        execute_options={"parameters": {"start": start, "end": cutoff}},
    )

    if df.height < 50:
        raise RuntimeError("Недостаточно данных для обучения")

    features = ["avg_temperature", "avg_pressure", "energy_kwh", "downtime_hours"]
    target = "oil_ton"

    train_df = df.drop_nulls(subset=features + [target])
    X = train_df.select(features).to_numpy()
    y = train_df.select(target).to_numpy().ravel()

    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X, y)

    last_day = cutoff
    test = df.filter(pl.col("date") == last_day)
    if test.height > 0:
        X_test = test.select(features).to_numpy()
        y_test = test.select(target).to_numpy().ravel()
        mae = mean_absolute_error(y_test, model.predict(X_test))
        logger.info("MAE на %s: %.3f", last_day, mae)

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    logger.info("Модель дебита сохранена в %s", MODEL_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=365)
    args = parser.parse_args()
    train(args.days_back)
