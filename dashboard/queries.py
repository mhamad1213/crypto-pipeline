"""Read-only queries for the dashboard.

Kept apart from app.py (and free of Streamlit) so the tests can call them directly.
"""

import os
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOCAL_URL = "postgresql+psycopg://crypto:crypto@localhost:5432/crypto"

# Fixed coin order and colours: a coin keeps its colour on every chart.
COIN_ORDER = ["BTC", "ETH", "SOL", "XRP", "ADA"]
COIN_COLOURS = {
    "BTC": "#2a78d6",
    "ETH": "#eb6834",
    "SOL": "#1baf7a",
    "XRP": "#eda100",
    "ADA": "#e87ba4",
}


def normalise_url(url: str) -> str:
    """Neon gives postgresql:// URLs; SQLAlchemy needs to be told to use psycopg."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def database_url() -> str:
    return normalise_url(os.getenv("DATABASE_URL", "").strip() or LOCAL_URL)


def get_engine(url: str) -> Engine:
    return create_engine(normalise_url(url), pool_pre_ping=True)


def _schemas() -> tuple[str, str]:
    # Overridable so tests can point at a throwaway schema.
    raw = os.getenv("DASHBOARD_RAW_SCHEMA", "public")
    analytics = os.getenv("DASHBOARD_ANALYTICS_SCHEMA", "analytics")
    return raw, analytics


def coin_summary(engine: Engine) -> pd.DataFrame:
    """One row per coin from dbt's coin_summary, in COIN_ORDER."""
    _, analytics = _schemas()
    df = pd.read_sql(text(f"SELECT * FROM {analytics}.coin_summary"), engine)
    df["order"] = df["symbol"].map({s: i for i, s in enumerate(COIN_ORDER)})
    return df.sort_values("order").drop(columns="order").reset_index(drop=True)


def daily_metrics(engine: Engine) -> pd.DataFrame:
    """Every coin-day from dbt's fct_daily_metrics (about 1,800 rows: small enough to load)."""
    _, analytics = _schemas()
    sql = f"""
        SELECT symbol, price_date, price_gbp, daily_return, ma_7d, ma_30d,
               volatility_30d_annualised, drawdown
        FROM {analytics}.fct_daily_metrics
        ORDER BY symbol, price_date
    """
    df = pd.read_sql(text(sql), engine, parse_dates=["price_date"])
    return df


def recent_runs(engine: Engine, limit: int = 14) -> pd.DataFrame:
    raw, _ = _schemas()
    sql = f"""
        SELECT run_id, started_at, finished_at, status, source,
               rows_inserted, rows_updated, error
        FROM {raw}.pipeline_runs
        ORDER BY run_id DESC
        LIMIT :limit
    """
    return pd.read_sql(text(sql), engine, params={"limit": limit})


def latest_checks(engine: Engine) -> pd.DataFrame:
    """The quality checks recorded by the most recent run that recorded any."""
    raw, _ = _schemas()
    sql = f"""
        SELECT run_id, stage, check_name, passed, details
        FROM {raw}.quality_results
        WHERE run_id = (SELECT MAX(run_id) FROM {raw}.quality_results)
        ORDER BY id
    """
    return pd.read_sql(text(sql), engine)


def total_rows(engine: Engine) -> int:
    raw, _ = _schemas()
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {raw}.raw_daily_prices")).scalar_one()


def rebased(metrics: pd.DataFrame, days: int) -> pd.DataFrame:
    """Each coin's price over the last `days` days, scaled so the first day = 100.

    This puts £61,000 Bitcoin and £0.17 Cardano on one chart: 110 means "up 10%".
    """
    start = metrics["price_date"].max() - pd.Timedelta(days=days - 1)
    window = metrics[metrics["price_date"] >= start].copy()
    first = window.groupby("symbol")["price_gbp"].transform("first")
    window["index_100"] = window["price_gbp"] / first * 100
    return window


def health(runs: pd.DataFrame, now: datetime | None = None) -> dict:
    """Headline numbers for the pipeline health panel, from recent_runs()."""
    now = now or datetime.now(timezone.utc)
    if runs.empty:
        return {"last_status": "no runs", "hours_since_success": None, "success_rate": None}
    ok = runs[runs["status"] == "success"]
    hours = None
    if not ok.empty:
        last_ok = pd.Timestamp(ok["finished_at"].max())
        if last_ok.tzinfo is None:
            last_ok = last_ok.tz_localize("UTC")
        hours = (pd.Timestamp(now) - last_ok).total_seconds() / 3600
    return {
        "last_status": runs.iloc[0]["status"],
        "hours_since_success": hours,
        "success_rate": len(ok) / len(runs),
    }
