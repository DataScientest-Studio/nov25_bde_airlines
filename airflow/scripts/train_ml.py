import os
from pathlib import Path

import joblib
import pandas as pd
import psycopg2

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor


# =====================
# ENV
# =====================
PG_HOST = os.getenv("PG_HOST", "postgres_data")
PG_DB = os.getenv("PG_DB", "flight")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_PORT = int(os.getenv("PG_PORT", "5432"))

MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/models")
MODEL_NAME = os.getenv("MODEL_NAME", "delay_tree_regressor.pkl")
MODEL_PATH = os.path.join(MODEL_DIR, MODEL_NAME)

RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))
TEST_SIZE = float(os.getenv("TEST_SIZE", "0.2"))

print(f" MODEL_PATH = {MODEL_PATH}")
print(f"Postgres = {PG_HOST}:{PG_PORT}/{PG_DB} user={PG_USER}")


def connect_pg():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )


def load_training_data(conn) -> pd.DataFrame:
    """
    Features:
      airline, origin_airport, destination_airport,
      scheduled_dep_hour, day_of_week, month, duration
    Target:
      dep_delay
    """
    query = """
    SELECT
        al.iata_code AS airline,
        a_dep.iata_code AS origin_airport,
        a_arr.iata_code AS destination_airport,
        EXTRACT(HOUR  FROM fd.dep_time_utc)::int AS scheduled_dep_hour,
        EXTRACT(DOW   FROM fd.dep_time_utc)::int AS day_of_week,
        EXTRACT(MONTH FROM fd.dep_time_utc)::int AS month,
        fd.duration::int AS duration,
        fd.dep_delay::int AS dep_delay
    FROM flight_delays fd
    JOIN flights f ON fd.flight_id = f.flight_id
    LEFT JOIN airlines al ON f.airline_id = al.airline_id
    LEFT JOIN airports a_dep ON f.dep_airport_id = a_dep.airport_id
    LEFT JOIN airports a_arr ON f.arr_airport_id = a_arr.airport_id
    WHERE fd.dep_delay IS NOT NULL
      AND fd.dep_time_utc IS NOT NULL
      AND fd.duration IS NOT NULL
      AND al.iata_code IS NOT NULL
      AND a_dep.iata_code IS NOT NULL
      AND a_arr.iata_code IS NOT NULL;
    """
    return pd.read_sql(query, conn)


def main():
    # 1) Load data
    conn = connect_pg()
    df = load_training_data(conn)
    conn.close()

    print(f"Lignes récupérées: {len(df)}")
    if len(df) < 200:
        raise ValueError("Pas assez de données pour entraîner un modèle fiable (min ~200 lignes).")

    # 2) Split features/target
    feature_cols = [
        "airline",
        "origin_airport",
        "destination_airport",
        "scheduled_dep_hour",
        "day_of_week",
        "month",
        "duration",
    ]
    target_col = "dep_delay"

    X = df[feature_cols].copy()
    y = df[target_col].astype(float).copy()

    # 3) Preprocess
    categorical_cols = ["airline", "origin_airport", "destination_airport"]
    numeric_cols = ["scheduled_dep_hour", "day_of_week", "month", "duration"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
            ("num", "passthrough", numeric_cols),
        ]
    )

    # 4) Model
    model = RandomForestRegressor(
        n_estimators=300,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        max_depth=None,
        min_samples_leaf=2,
    )

    pipe = Pipeline(steps=[("prep", preprocessor), ("model", model)])

    # 5) Train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    pipe.fit(X_train, y_train)

    # 6) Metrics
    preds = pipe.predict(X_test)
    mae = float(mean_absolute_error(y_test, preds))
    rmse = float(mean_squared_error(y_test, preds, squared=False))
    r2 = float(r2_score(y_test, preds))

    print("✅ Training terminé (target=dep_delay)")
    print(f"   MAE  = {mae:.2f} minutes")
    print(f"   RMSE = {rmse:.2f} minutes")
    print(f"   R²   = {r2:.3f}")

    # 7) Save
    Path(MODEL_DIR).mkdir(parents=True, exist_ok=True)

    payload = {
        "pipeline": pipe,
        "features": feature_cols,
        "target": target_col,
        "metrics": {"mae": mae, "rmse": rmse, "r2": r2},
        "trained_at_utc": pd.Timestamp.utcnow().isoformat(),
    }

    joblib.dump(payload, MODEL_PATH)
    print(f"Modèle sauvegardé: {MODEL_PATH}")


if __name__ == "__main__":
    main()
