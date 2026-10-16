import numpy as np
import pandas as pd
import pytest
from sqlalchemy import select

from crypto_pipeline import config, db, forecast


def fake_metrics(days: int = 300, coins: int = 3) -> pd.DataFrame:
    """Prices with volatility clustering (GARCH), shaped like analytics.fct_daily_metrics.

    Calm weeks follow calm weeks and wild weeks follow wild weeks, as in real crypto,
    so there is something real for a model to learn.
    """
    frames = []
    for k, coin_id in enumerate(list(config.COINS)[:coins]):
        rng = np.random.default_rng(k)
        var, returns = 1e-3, []
        for _ in range(days):
            r = rng.standard_t(5) / np.sqrt(5 / 3) * np.sqrt(var)
            var = 2e-5 * (1 + k) + 0.12 * r * r + 0.85 * var
            returns.append(r)
        price = 100 * np.exp(np.cumsum(returns))
        df = pd.DataFrame(
            {
                "coin_id": coin_id,
                "price_date": pd.date_range("2025-01-01", periods=days, freq="D"),
                "price_gbp": price,
            }
        )
        df["log_return"] = np.log(df["price_gbp"]).diff()
        df["volatility_7d"] = df["log_return"].rolling(7).std()
        df["volatility_30d"] = df["log_return"].rolling(30).std()
        df["drawdown"] = df["price_gbp"] / df["price_gbp"].cummax() - 1
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def data():
    return forecast.make_features(fake_metrics())


def test_target_is_the_next_seven_days(data):
    coin = data[data["coin_id"] == "bitcoin"].reset_index(drop=True)
    metrics = fake_metrics()
    returns = metrics[metrics["coin_id"] == "bitcoin"].set_index("price_date")["log_return"]
    row = coin.iloc[50]
    future = returns[returns.index > row.price_date].iloc[:7]  # tomorrow .. 7 days ahead
    assert row.target_vol == pytest.approx(future.std())
    assert row.target_end == future.index[-1]


def test_features_never_look_ahead():
    """Changing the future must not change today's features (no leakage)."""
    metrics = fake_metrics()
    changed = metrics.copy()
    later = changed["price_date"] > "2025-06-01"
    changed.loc[later, ["price_gbp", "log_return", "volatility_7d", "volatility_30d"]] *= 3
    cols = forecast.BOOST_FEATURES + ["naive_vol"]
    before = forecast.make_features(metrics)
    after = forecast.make_features(changed)
    upto = before["price_date"] <= "2025-06-01"
    pd.testing.assert_frame_equal(before.loc[upto, cols], after.loc[upto, cols])


def test_walk_forward_trains_only_on_finished_targets(data, monkeypatch):
    """Embargo: every training row's 7-day target ended before the test block started."""
    seen = []
    real_fit = forecast.fit

    def spy(name, train):
        seen.append(train["target_end"].max())
        return real_fit(name, train)

    monkeypatch.setattr(forecast, "fit", spy)
    result = forecast.walk_forward(data, "har_ridge")
    starts = result.groupby("train_rows")["price_date"].min().sort_values().to_list()
    assert len(seen) == len(starts) > 1
    for last_target_end, test_start in zip(seen, starts):
        assert last_target_end < test_start
    assert result["price_date"].min() > data["price_date"].min() + pd.Timedelta(days=100)


def test_har_beats_naive_on_clustered_volatility(data):
    table = forecast.evaluate(data, models=("naive", "har_ridge")).set_index("model")
    assert table.loc["naive", "vs_naive"] == 0
    assert table.loc["har_ridge", "vs_naive"] < 0  # lower error than "same as last week"


def test_too_little_history_is_a_clear_error():
    short = forecast.make_features(fake_metrics(days=100))
    with pytest.raises(ValueError, match="need more than 120 days"):
        forecast.walk_forward(short, "naive")


def test_save_keeps_live_forecasts_and_fills_actuals_later(engine):
    metrics = fake_metrics()
    cutoff = metrics["price_date"].max() - pd.Timedelta(days=10)
    early = forecast.make_features(metrics[metrics["price_date"] <= cutoff])
    full = forecast.make_features(metrics)
    evaluation = forecast.evaluate(early, models=("naive", "har_ridge"))

    assert forecast.save(engine, early, evaluation) == 3  # one live forecast per coin
    assert forecast.save(engine, early, evaluation) == 0  # same day again: nothing new
    p = db.predictions
    with engine.connect() as conn:
        first = conn.execute(select(p).where(p.c.kind == "live").order_by(p.c.coin_id)).all()
    assert len(first) == 3 and all(r.actual_vol is None for r in first)

    # 10 days later: new forecasts for today, and last time's now have an actual value.
    assert forecast.save(engine, full, evaluation) == 3
    with engine.connect() as conn:
        live = conn.execute(
            select(p).where(p.c.kind == "live").order_by(p.c.made_on, p.c.coin_id)
        ).all()
        backtests = conn.execute(select(p.c.made_on).where(p.c.kind == "backtest")).all()
        evaluations = conn.execute(select(db.forecast_evaluations)).all()
    old, new = live[:3], live[3:]
    assert all(r.made_on == cutoff.date() and r.actual_vol > 0 for r in old)
    assert all(r.actual_vol is None for r in new)
    assert [r.predicted_vol for r in old] == [r.predicted_vol for r in first]  # never rewritten
    assert len(backtests) == len(forecast.walk_forward(full, "har_ridge"))  # replaced, not added
    assert len(evaluations) == 6  # 2 models x 3 saves
