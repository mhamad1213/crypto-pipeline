"""Forecast next week's volatility for each coin, and check it fairly against a naive baseline.

    python -m crypto_pipeline.forecast            walk-forward evaluation, prints a table
    python -m crypto_pipeline.forecast --save     also write forecasts to the database

The question: "how much will this coin's price jump around over the next 7 days?"
  target  = volatility of the next 7 daily log returns (days t+1 .. t+7), annualised
  naive   = "same as the last 7 days" (volatility_7d today): the baseline to beat
  models  = HAR-style Ridge regression on log volatilities, and gradient boosting
"""

import argparse

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import delete, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from crypto_pipeline import db

HORIZON = 7  # forecast the volatility of the next 7 days
FIRST_TEST_DAYS = 120  # walk-forward: the first ~4 months are only ever used for training
STEP_DAYS = 30  # retrain once a month during the walk-forward test
ANNUALISE = np.sqrt(365)
EPS = 1e-4  # stops log(0) on a day the price didn't move
PRODUCTION_MODEL = "har_ridge"

HAR_FEATURES = ["log_vol_1d", "log_vol_7d", "log_vol_30d"]
BOOST_FEATURES = HAR_FEATURES + ["log_return", "return_7d", "drawdown"]


def load_metrics(engine: Engine, schema: str = "analytics") -> pd.DataFrame:
    sql = f"""
        SELECT coin_id, price_date, price_gbp, log_return, volatility_7d, volatility_30d, drawdown
        FROM {schema}.fct_daily_metrics
        ORDER BY coin_id, price_date
    """
    return pd.read_sql(text(sql), engine, parse_dates=["price_date"])


def make_features(metrics: pd.DataFrame) -> pd.DataFrame:
    """One row per coin-day: features known at the end of that day, and the future target.

    Features only look backwards (today and earlier). The target only looks forwards
    (tomorrow onwards). Mixing those up is the classic way to fool yourself.
    """
    df = metrics.sort_values(["coin_id", "price_date"]).copy()
    by_coin = df.groupby("coin_id")
    df["log_vol_1d"] = np.log(df["log_return"].abs() + EPS)
    df["log_vol_7d"] = np.log(df["volatility_7d"] + EPS)
    df["log_vol_30d"] = np.log(df["volatility_30d"] + EPS)
    df["return_7d"] = by_coin["price_gbp"].pct_change(7)
    # Target: std of the next 7 log returns = the 7-day volatility measured 7 days from now.
    df["target_vol"] = by_coin["volatility_7d"].shift(-HORIZON)
    df["naive_vol"] = df["volatility_7d"]
    df["target_end"] = by_coin["price_date"].shift(-HORIZON)
    needed = BOOST_FEATURES + ["naive_vol"]
    return df.dropna(subset=needed).reset_index(drop=True)


def build_model(name: str):
    if name == "har_ridge":
        # HAR: tomorrow's volatility ≈ a mix of yesterday's, last week's and last month's.
        return make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    if name == "boosting":
        return HistGradientBoostingRegressor(
            max_iter=200,
            learning_rate=0.05,
            max_leaf_nodes=15,
            min_samples_leaf=40,
            random_state=0,
        )
    raise ValueError(f"unknown model {name!r}")


def features_for(name: str) -> list[str]:
    return HAR_FEATURES if name == "har_ridge" else BOOST_FEATURES


def fit(name: str, train: pd.DataFrame):
    model = build_model(name)
    model.fit(train[features_for(name)], np.log(train["target_vol"]))
    return model


def predict(name: str, model, rows: pd.DataFrame) -> np.ndarray:
    return np.exp(model.predict(rows[features_for(name)]))


def walk_forward(data: pd.DataFrame, name: str) -> pd.DataFrame:
    """Test the way the model would really have been used: always trained on the past only.

    Every 30 days: train on all rows whose 7-day target had *finished* before the test
    block starts (that 7-day gap is the "embargo"), then predict the next 30 days.
    """
    labelled = data.dropna(subset=["target_vol"])
    dates = np.sort(labelled["price_date"].unique())
    if len(dates) <= FIRST_TEST_DAYS:
        raise ValueError(f"need more than {FIRST_TEST_DAYS} days of history, have {len(dates)}")
    out = []
    for start in range(FIRST_TEST_DAYS, len(dates), STEP_DAYS):
        test_start = dates[start]
        test_end = dates[min(start + STEP_DAYS, len(dates)) - 1]
        cutoff = test_start - np.timedelta64(HORIZON, "D")  # target must be known by test_start
        train = labelled[labelled["price_date"] < cutoff]
        test = labelled[
            (labelled["price_date"] >= test_start) & (labelled["price_date"] <= test_end)
        ]
        if name == "naive":
            pred = test["naive_vol"].to_numpy()
        else:
            pred = predict(name, fit(name, train), test)
        out.append(test.assign(model=name, predicted_vol=pred, train_rows=len(train)))
    return pd.concat(out, ignore_index=True)


def evaluate(data: pd.DataFrame, models=("naive", "har_ridge", "boosting")) -> pd.DataFrame:
    """MAE of each model on the walk-forward test, in annualised volatility points."""
    rows = []
    for name in models:
        wf = walk_forward(data, name)
        err = (wf["predicted_vol"] - wf["target_vol"]).abs() * ANNUALISE
        rows.append({"model": name, "mae": err.mean(), "n": len(wf)})
    table = pd.DataFrame(rows)
    naive = table.loc[table["model"] == "naive", "mae"].iloc[0]
    table["vs_naive"] = table["mae"] / naive - 1  # negative = better than naive
    return table


def latest_forecasts(data: pd.DataFrame, name: str = PRODUCTION_MODEL) -> pd.DataFrame:
    """Train on everything labelled so far, then forecast from each coin's newest day."""
    labelled = data.dropna(subset=["target_vol"])
    model = fit(name, labelled)
    newest = data[data["price_date"] == data.groupby("coin_id")["price_date"].transform("max")]
    return newest.assign(model=name, predicted_vol=predict(name, model, newest))


def _rows(df: pd.DataFrame, kind: str) -> list[dict]:
    return [
        {
            "coin_id": r.coin_id,
            "made_on": r.price_date.date(),
            "kind": kind,
            "model": r.model,
            "predicted_vol": float(r.predicted_vol * ANNUALISE),
            "naive_vol": float(r.naive_vol * ANNUALISE),
            "actual_vol": None if pd.isna(r.target_vol) else float(r.target_vol * ANNUALISE),
        }
        for r in df.itertuples()
    ]


def save(engine: Engine, data: pd.DataFrame, evaluation: pd.DataFrame, run_id=None) -> int:
    """Write forecasts. Returns how many new live forecasts were stored.

    - backtest rows (the walk-forward test) are replaced every run;
    - live rows are written once per coin per day and never changed afterwards, except
      that `actual_vol` is filled in once the 7 days have happened.
    """
    db.create_tables(engine)
    backtest = walk_forward(data, PRODUCTION_MODEL)
    live = latest_forecasts(data)
    p = db.predictions
    with engine.begin() as conn:
        conn.execute(delete(p).where(p.c.kind == "backtest"))
        conn.execute(insert(p), _rows(backtest, "backtest"))
        new = conn.execute(
            insert(p).values(_rows(live, "live")).on_conflict_do_nothing().returning(p.c.coin_id)
        ).all()
        # Fill in what actually happened for past live forecasts whose 7 days are over.
        waiting = conn.execute(
            select(p.c.coin_id, p.c.made_on).where((p.c.kind == "live") & p.c.actual_vol.is_(None))
        ).all()
        actual = data.dropna(subset=["target_vol"]).set_index(["coin_id", "price_date"])
        for coin_id, made_on in waiting:
            key = (coin_id, pd.Timestamp(made_on))
            if key in actual.index:
                conn.execute(
                    update(p)
                    .where(
                        (p.c.kind == "live") & (p.c.coin_id == coin_id) & (p.c.made_on == made_on)
                    )
                    .values(actual_vol=float(actual.loc[key, "target_vol"] * ANNUALISE))
                )
        conn.execute(
            insert(db.forecast_evaluations),
            [
                {
                    "run_id": run_id,
                    "model": e.model,
                    "mae": float(e.mae),
                    "vs_naive": float(e.vs_naive),
                    "n": int(e.n),
                }
                for e in evaluation.itertuples()
            ],
        )
    return len(new)


def run(engine: Engine, run_id=None, schema: str = "analytics") -> str:
    """Evaluate, forecast and save. Used by the daily flow. Returns a one-line summary."""
    data = make_features(load_metrics(engine, schema))
    evaluation = evaluate(data)
    new = save(engine, data, evaluation, run_id)
    best = evaluation.set_index("model").loc[PRODUCTION_MODEL]
    return (
        f"{new} new forecasts, {PRODUCTION_MODEL} MAE {best.mae:.1%} "
        f"({best.vs_naive:+.0%} vs naive)"
    )


def print_report(data: pd.DataFrame, evaluation: pd.DataFrame) -> None:
    labelled = data.dropna(subset=["target_vol"])
    print(
        f"{len(labelled):,} labelled coin-days, {labelled['coin_id'].nunique()} coins, "
        f"{labelled['price_date'].min():%Y-%m-%d} -> {labelled['price_date'].max():%Y-%m-%d}"
    )
    print("\nWalk-forward test (MAE in annualised volatility points, lower is better):")
    for e in evaluation.itertuples():
        print(f"  {e.model:<10} MAE {e.mae:6.1%}   vs naive {e.vs_naive:+6.1%}   ({e.n} forecasts)")
    print(f"\nForecasts for the next {HORIZON} days ({PRODUCTION_MODEL}):")
    for r in latest_forecasts(data).itertuples():
        print(
            f"  {r.coin_id:<9} last 7 days {r.naive_vol * ANNUALISE:6.1%}  ->  "
            f"next 7 days {r.predicted_vol * ANNUALISE:6.1%}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Volatility forecast vs naive baseline")
    parser.add_argument("--save", action="store_true", help="write forecasts to the database")
    args = parser.parse_args()
    engine = db.get_engine()
    data = make_features(load_metrics(engine))
    evaluation = evaluate(data)
    print_report(data, evaluation)
    if args.save:
        new = save(engine, data, evaluation)
        print(f"\nsaved: {new} new live forecasts")


if __name__ == "__main__":
    main()
