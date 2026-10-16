from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import insert, text
from streamlit.testing.v1 import AppTest

import queries
from crypto_pipeline import db

SYMBOLS = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP", "cardano": "ADA"}
NOW = datetime(2026, 10, 16, 7, 0, tzinfo=timezone.utc)


def fake_metrics(days: int = 40) -> pd.DataFrame:
    """Prices that grow 1% a day from 100, for every coin."""
    rows = []
    for coin, symbol in SYMBOLS.items():
        for i in range(days):
            rows.append(
                {
                    "coin_id": coin,
                    "symbol": symbol,
                    "price_date": date(2026, 9, 1) + timedelta(days=i),
                    "price_gbp": 100 * 1.01**i,
                    "daily_return": 0.01 if i else None,
                    "ma_7d": None,
                    "ma_30d": None,
                    "volatility_30d_annualised": 0.5,
                    "drawdown": 0.0,
                }
            )
    return pd.DataFrame(rows)


def fake_summary() -> pd.DataFrame:
    # Deliberately not in BTC..ADA order, to check the dashboard sorts it.
    return pd.DataFrame(
        {
            "coin_id": list(reversed(SYMBOLS)),
            "symbol": list(reversed(SYMBOLS.values())),
            "latest_price_gbp": [0.17, 1.01, 78.0, 1829.0, 61682.0],
            "return_7d": [0.01] * 5,
            "return_30d": [0.05] * 5,
            "volatility_30d_annualised": [0.8, 0.6, 0.6, 0.45, 0.39],
            "current_drawdown": [-0.6] * 5,
            "max_drawdown": [-0.7] * 5,
        }
    )


def runs_frame(statuses: list[str]) -> pd.DataFrame:
    """Newest first, one hour apart, like recent_runs() returns."""
    n = len(statuses)
    return pd.DataFrame(
        {
            "run_id": list(range(n, 0, -1)),
            "status": statuses,
            "finished_at": [NOW - timedelta(hours=h + 1) for h in range(n)],
        }
    )


@pytest.fixture
def dashboard_db(engine, monkeypatch):
    """Raw tables + fake dbt tables in the throwaway test schema."""
    with engine.connect() as conn:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    monkeypatch.setenv("DASHBOARD_RAW_SCHEMA", schema)
    monkeypatch.setenv("DASHBOARD_ANALYTICS_SCHEMA", schema)
    db.create_tables(engine)
    fake_metrics().to_sql("fct_daily_metrics", engine, schema=schema, index=False)
    fake_summary().to_sql("coin_summary", engine, schema=schema, index=False)
    with engine.begin() as conn:
        conn.execute(
            insert(db.pipeline_runs),
            [
                {
                    "status": "success",
                    "source": "api",
                    "rows_inserted": 5,
                    "rows_updated": 1820,
                    "finished_at": NOW,
                },
                {
                    "status": "failed",
                    "source": "api",
                    "rows_inserted": 0,
                    "rows_updated": 0,
                    "error": "PermanentCoinGeckoError: 401",
                    "finished_at": NOW,
                },
            ],
        )
        conn.execute(
            insert(db.quality_results),
            [
                {"run_id": 1, "stage": "batch", "check_name": "batch_schema", "passed": True},
                {"run_id": 2, "stage": "batch", "check_name": "batch_schema", "passed": False},
                {"run_id": 2, "stage": "table", "check_name": "data_is_fresh", "passed": True},
            ],
        )
        day = date(2026, 10, 1)
        conn.execute(
            insert(db.predictions),
            [
                # backtest and live for the same day: the dashboard must show the live one
                {
                    "coin_id": "bitcoin",
                    "made_on": day,
                    "kind": "backtest",
                    "model": "har_ridge",
                    "predicted_vol": 0.30,
                    "naive_vol": 0.25,
                    "actual_vol": 0.35,
                },
                {
                    "coin_id": "bitcoin",
                    "made_on": day,
                    "kind": "live",
                    "model": "har_ridge",
                    "predicted_vol": 0.32,
                    "naive_vol": 0.25,
                    "actual_vol": 0.35,
                },
            ]
            + [
                {
                    "coin_id": coin,
                    "made_on": day + timedelta(days=7),
                    "kind": "live",
                    "model": "har_ridge",
                    "predicted_vol": 0.40 + i / 10,
                    "naive_vol": 0.30,
                    "actual_vol": None,
                }
                for i, coin in enumerate(SYMBOLS)
            ],
        )
        conn.execute(
            insert(db.forecast_evaluations),
            [
                {"model": "naive", "mae": 0.33, "vs_naive": 0.0, "n": 1040},
                {"model": "har_ridge", "mae": 0.30, "vs_naive": -0.094, "n": 1040},
            ],
        )
    return engine


def test_neon_url_gets_psycopg_driver():
    assert queries.normalise_url("postgresql://u:p@h/db") == "postgresql+psycopg://u:p@h/db"
    assert queries.normalise_url("postgres://u:p@h/db") == "postgresql+psycopg://u:p@h/db"


def test_rebased_starts_every_coin_at_100():
    out = queries.rebased(
        fake_metrics().assign(price_date=lambda d: pd.to_datetime(d.price_date)), 30
    )
    first = out.sort_values("price_date").groupby("symbol")["index_100"].first()
    last = out.sort_values("price_date").groupby("symbol")["index_100"].last()
    assert first.round(6).eq(100).all()
    assert last.round(2).eq(round(100 * 1.01**29, 2)).all()  # 29 days of +1%
    assert out.groupby("symbol").size().eq(30).all()


def test_health_when_latest_run_succeeded():
    h = queries.health(runs_frame(["success", "failed", "success"]), now=NOW)
    assert h["last_status"] == "success"
    assert h["hours_since_success"] == pytest.approx(1)
    assert h["success_rate"] == pytest.approx(2 / 3)


def test_health_counts_hours_since_last_success_after_failures():
    h = queries.health(runs_frame(["failed", "failed", "success"]), now=NOW)
    assert h["last_status"] == "failed"
    assert h["hours_since_success"] == pytest.approx(3)


def test_health_with_no_runs():
    assert (
        queries.health(pd.DataFrame(columns=["status", "finished_at"]))["last_status"] == "no runs"
    )


def test_queries_read_the_tables(dashboard_db):
    summary = queries.coin_summary(dashboard_db)
    assert list(summary["symbol"]) == queries.COIN_ORDER  # sorted BTC..ADA
    assert len(queries.daily_metrics(dashboard_db)) == 5 * 40
    assert list(queries.recent_runs(dashboard_db)["status"]) == ["failed", "success"]
    checks = queries.latest_checks(dashboard_db)
    assert set(checks["run_id"]) == {2}  # only the newest run's checks
    assert queries.total_rows(dashboard_db) == 0


def test_forecast_queries(dashboard_db):
    fc = queries.forecasts(dashboard_db)
    assert len(fc) == 1 + 5  # one row per coin-day: live wins over backtest
    first = fc[fc["made_on"] == "2026-10-01"].iloc[0]
    assert (first.kind, first.predicted_vol) == ("live", 0.32)
    assert fc["actual_vol"].isna().sum() == 5  # the newest 7 days haven't happened yet
    assert list(queries.latest_evaluation(dashboard_db)["model"]) == ["har_ridge", "naive"]


def test_forecast_queries_before_first_forecast(engine, monkeypatch):
    with engine.connect() as conn:
        monkeypatch.setenv(
            "DASHBOARD_RAW_SCHEMA", conn.execute(text("SELECT current_schema()")).scalar_one()
        )
    assert queries.forecasts(engine).empty  # no tables yet: empty, not a crash
    assert queries.latest_evaluation(engine).empty


def test_app_renders_without_errors(dashboard_db):
    at = AppTest.from_file("../dashboard/app.py", default_timeout=60).run()
    assert not at.exception
    assert [m.label for m in at.metric][:5] == queries.COIN_ORDER
    assert at.metric[0].value == "£61,682.00"
    assert at.metric[4].value == "£0.1700"
    outlook = {m.label: m.value for m in at.metric[5:10]}
    assert outlook == {"BTC": "40%", "ETH": "50%", "SOL": "60%", "XRP": "70%", "ADA": "80%"}
    health = {m.label: m.value for m in at.metric[10:]}
    assert health["Last run"] == "❌ failed"
    assert health["Checks passed (latest run)"] == "1 / 2"
