import json
from datetime import date, timedelta

import pytest
from sqlalchemy import delete, select

from crypto_pipeline import config, db, extract, load, quality
from crypto_pipeline.models import DailyPrice

TODAY = date(2026, 10, 13)


def make_rows(coin_id="bitcoin", days=5, start=date(2026, 10, 9), price=100.0):
    return [
        DailyPrice(
            coin_id=coin_id, day=start + timedelta(days=i), price=price, market_cap=1e9, volume=1e8
        )
        for i in range(days)
    ]


def failed_names(results):
    return {r.name for r in results if not r.passed}


# ---------- batch checks ----------


def test_good_batch_passes(btc_payload):
    df = quality.rows_to_frame(extract.parse("bitcoin", btc_payload))
    results = quality.check_batch(df)
    assert [r.passed for r in results] == [True]


def test_batch_catches_bad_values():
    df = quality.rows_to_frame(make_rows() + make_rows()[:1])  # 9 Oct twice
    df.loc[2, "price_gbp"] = 0.0  # zero price
    df.loc[3, "volume_gbp"] = -5.0  # negative volume
    df.loc[4, "coin_id"] = "dogecoin"  # coin we don't track
    assert failed_names(quality.check_batch(df)) >= {
        "one_row_per_coin_day",
        "price_positive",
        "volume_not_negative",
        "known_coin",
    }


@pytest.mark.parametrize("jump, ok", [(1.49, True), (1.51, False), (0.51, True), (0.49, False)])
def test_daily_move_limit(jump, ok):
    df = quality.rows_to_frame(make_rows(days=3))
    df.loc[2, "price_gbp"] = 100.0 * jump  # day 3 vs day 2
    assert ("daily_move_under_50pct" not in failed_names(quality.check_batch(df))) == ok


def test_daily_move_is_per_coin():
    # Bitcoin at £100 next to Cardano at £0.30 is fine: moves are compared within each coin
    df = quality.rows_to_frame(make_rows("bitcoin") + make_rows("cardano", price=0.30))
    assert failed_names(quality.check_batch(df)) == set()


# ---------- table checks ----------


def test_table_checks_pass_on_complete_fresh_data(engine):
    db.create_tables(engine)
    db.upsert_prices(engine, make_rows())  # 9-13 Oct, no gaps
    assert failed_names(quality.check_table(engine, ["bitcoin"], TODAY)) == set()


def test_missing_day_is_caught(engine):
    db.create_tables(engine)
    db.upsert_prices(engine, make_rows())
    with engine.begin() as conn:
        conn.execute(
            delete(db.raw_daily_prices).where(db.raw_daily_prices.c.day == date(2026, 10, 11))
        )
    results = quality.check_table(engine, ["bitcoin"], TODAY)
    assert failed_names(results) == {"no_missing_days"}
    assert "bitcoin 2026-10-11" in next(r.details for r in results if not r.passed)


def test_stale_data_is_caught(engine):
    db.create_tables(engine)
    db.upsert_prices(engine, make_rows())  # newest day 13 Oct
    assert (
        failed_names(quality.check_table(engine, ["bitcoin"], TODAY + timedelta(days=1))) == set()
    )
    assert failed_names(quality.check_table(engine, ["bitcoin"], TODAY + timedelta(days=2))) == {
        "data_is_fresh"
    }


def test_missing_coin_is_caught(engine):
    db.create_tables(engine)
    db.upsert_prices(engine, make_rows("bitcoin"))
    results = quality.check_table(engine, ["bitcoin", "ethereum"], TODAY)
    assert failed_names(results) == {"all_coins_present"}


# ---------- checks inside a full run ----------


def test_bad_batch_blocks_the_whole_load(engine, btc_payload, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    btc_payload["prices"][2][1] = btc_payload["prices"][1][1] * 3  # a 200% jump on 11 Oct
    extract.save_raw("bitcoin", btc_payload)

    with pytest.raises(quality.DataQualityError, match="daily_move_under_50pct"):
        load.run(engine, ["bitcoin"], from_raw=True, today=TODAY)

    assert load.table_count(engine) == 0  # nothing was loaded
    with engine.connect() as conn:
        run = conn.execute(select(db.pipeline_runs)).one()
        checks = conn.execute(select(db.quality_results)).all()
    assert run.status == "failed" and "DataQualityError" in run.error
    assert [(c.run_id, c.check_name, c.passed) for c in checks] == [
        (run.run_id, "daily_move_under_50pct", False)
    ]


def test_stale_table_fails_run_but_keeps_rows(engine, btc_payload, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    (tmp_path / "bitcoin").mkdir()
    (tmp_path / "bitcoin" / "2026-10-14T000000Z.json").write_text(json.dumps(btc_payload))

    with pytest.raises(quality.DataQualityError, match="data_is_fresh"):
        load.run(engine, ["bitcoin"], from_raw=True, today=date(2026, 10, 20))

    assert load.table_count(engine) == 5  # good rows stay; the alarm is raised on the run
    with engine.connect() as conn:
        run = conn.execute(select(db.pipeline_runs)).one()
        checks = conn.execute(select(db.quality_results.c.stage, db.quality_results.c.passed)).all()
    assert run.status == "failed"
    assert sorted(checks) == [("batch", True), ("table", False), ("table", True), ("table", True)]
