from datetime import date
from types import SimpleNamespace

import pytest
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy import select

from crypto_pipeline import config, db, extract, load
from crypto_pipeline import flow as pipeline
from crypto_pipeline.coingecko import CoinGeckoError, PermanentCoinGeckoError

TODAY = date(2026, 10, 13)


@pytest.fixture(scope="module", autouse=True)
def prefect_server():
    """A throwaway Prefect server just for these tests (takes a few seconds to start)."""
    with prefect_test_harness():
        yield


@pytest.fixture
def setup(engine, btc_payload, tmp_path, monkeypatch):
    """Point the flow at the test schema and a temp raw folder holding one bitcoin file."""
    monkeypatch.setattr(pipeline.db, "get_engine", lambda: engine)
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    extract.save_raw("bitcoin", btc_payload)
    return engine


class FakeDbt:
    """Stands in for dbtRunner so tests don't need the real dbt project."""

    def __init__(self, success: bool, statuses: list[str]):
        self.success, self.statuses = success, statuses

    def invoke(self, args):
        results = [SimpleNamespace(status=s) for s in self.statuses]
        return SimpleNamespace(
            success=self.success, exception=None, result=SimpleNamespace(results=results)
        )


def runs_and_checks(engine):
    with engine.connect() as conn:
        runs = conn.execute(select(db.pipeline_runs)).all()
        checks = conn.execute(select(db.quality_results.c.stage, db.quality_results.c.passed)).all()
    return runs, checks


def test_flow_from_raw_without_dbt(setup):
    result = pipeline.daily_pipeline(["bitcoin"], from_raw=True, run_dbt=False, today=TODAY)
    assert (result["inserted"], result["dbt"]) == (5, "skipped")
    runs, checks = runs_and_checks(setup)
    assert [r.status for r in runs] == ["success"]
    assert {stage for stage, _ in checks} == {"batch", "table"}


def test_flow_records_dbt_success(setup, monkeypatch):
    monkeypatch.setattr(
        pipeline, "dbtRunner", lambda: FakeDbt(True, ["success"] * 4 + ["pass"] * 20)
    )
    monkeypatch.setattr(pipeline.forecast, "run", lambda engine, run_id: "5 new forecasts")
    result = pipeline.daily_pipeline(["bitcoin"], from_raw=True, today=TODAY)
    assert result["dbt"] == "pass=20, success=4"
    assert result["forecast"] == "5 new forecasts"
    _, checks = runs_and_checks(setup)
    assert ("dbt", True) in checks and ("forecast", True) in checks


def test_forecast_failure_fails_the_run(setup, monkeypatch):
    def broken(engine, run_id):
        raise ValueError("need more than 120 days of history, have 30")

    monkeypatch.setattr(pipeline, "dbtRunner", lambda: FakeDbt(True, ["success"]))
    monkeypatch.setattr(pipeline.forecast, "run", broken)
    with pytest.raises(ValueError, match="120 days"):
        pipeline.daily_pipeline(["bitcoin"], from_raw=True, today=TODAY)
    runs, _ = runs_and_checks(setup)
    assert runs[0].status == "failed" and "ValueError" in runs[0].error


def test_dbt_failure_fails_the_run(setup, monkeypatch):
    monkeypatch.setattr(pipeline, "dbtRunner", lambda: FakeDbt(False, ["fail", "pass", "skipped"]))
    with pytest.raises(pipeline.DbtBuildError, match="fail=1, pass=1, skipped=1"):
        pipeline.daily_pipeline(["bitcoin"], from_raw=True, today=TODAY)
    runs, checks = runs_and_checks(setup)
    assert runs[0].status == "failed" and "DbtBuildError" in runs[0].error
    assert runs[0].rows_inserted == 5  # data was loaded; only the dbt step failed
    assert ("dbt", False) in checks


def test_fetch_is_retried_by_prefect(setup, btc_payload, monkeypatch):
    calls = []

    def flaky_extract(client, coin_id):
        calls.append(coin_id)
        if len(calls) == 1:
            raise CoinGeckoError("gave up after 3 attempts: HTTP 503")  # first attempt fails
        return extract.parse(coin_id, btc_payload)

    monkeypatch.setenv("COINGECKO_API_KEY", "test-key")
    monkeypatch.setattr(pipeline.extract, "extract_coin", flaky_extract)
    monkeypatch.setattr(
        pipeline, "fetch_coin", pipeline.fetch_coin.with_options(retry_delay_seconds=0)
    )

    result = pipeline.daily_pipeline(["bitcoin"], run_dbt=False, today=TODAY)
    assert calls == ["bitcoin", "bitcoin"]  # failed once, retried, succeeded
    assert result["inserted"] == 5
    assert load.table_count(setup) == 5


def test_wrong_api_key_is_not_retried(setup, monkeypatch):
    calls = []

    def bad_key(client, coin_id):
        calls.append(coin_id)
        raise PermanentCoinGeckoError("401: check your API key")

    monkeypatch.setenv("COINGECKO_API_KEY", "wrong-key")
    monkeypatch.setattr(pipeline.extract, "extract_coin", bad_key)
    monkeypatch.setattr(
        pipeline, "fetch_coin", pipeline.fetch_coin.with_options(retry_delay_seconds=0)
    )

    with pytest.raises(PermanentCoinGeckoError):
        pipeline.daily_pipeline(["bitcoin"], run_dbt=False, today=TODAY)
    assert calls == ["bitcoin"]  # tried once, no pointless retries
    runs, _ = runs_and_checks(setup)
    assert runs[0].status == "failed" and "check your API key" in runs[0].error


def test_bad_batch_stops_flow_before_load(setup, btc_payload, monkeypatch):
    btc_payload["prices"][2][1] *= 3
    extract.save_raw("bitcoin", btc_payload)  # newer file with a tripled price
    monkeypatch.setattr(pipeline, "dbtRunner", lambda: pytest.fail("dbt must not run"))
    with pytest.raises(Exception, match="daily_move_under_50pct"):
        pipeline.daily_pipeline(["bitcoin"], from_raw=True, today=TODAY)
    assert load.table_count(setup) == 0
