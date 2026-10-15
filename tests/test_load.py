from datetime import date

import pytest
from sqlalchemy import select

from crypto_pipeline import config, db, extract, load
from crypto_pipeline.models import DailyPrice


def make_rows(price: float = 100.0) -> list[DailyPrice]:
    return [
        DailyPrice(
            coin_id="bitcoin", day=date(2026, 10, d), price=price + d, market_cap=1e9, volume=1e8
        )
        for d in (11, 12, 13)
    ]


def test_create_tables_is_safe_to_repeat(engine):
    db.create_tables(engine)
    db.create_tables(engine)
    with engine.connect() as conn:
        assert len(conn.execute(select(db.coins)).all()) == len(config.COINS)


def test_upsert_twice_creates_no_duplicates(engine):
    db.create_tables(engine)
    assert db.upsert_prices(engine, make_rows()) == (3, 0)  # 3 inserted
    assert db.upsert_prices(engine, make_rows()) == (0, 3)  # same rows again: 3 updated
    assert load.table_count(engine) == 3


def test_upsert_overwrites_changed_values(engine):
    db.create_tables(engine)
    db.upsert_prices(engine, make_rows(price=100.0))
    db.upsert_prices(engine, make_rows(price=500.0))  # e.g. CoinGecko corrected a price
    with engine.connect() as conn:
        prices = (
            conn.execute(
                select(db.raw_daily_prices.c.price_gbp).order_by(db.raw_daily_prices.c.day)
            )
            .scalars()
            .all()
        )
    assert prices == [511.0, 512.0, 513.0]


def test_run_from_raw_records_successful_run(engine, btc_payload, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    extract.save_raw("bitcoin", btc_payload)

    first = load.run(engine, ["bitcoin"], from_raw=True, today=date(2026, 10, 13))
    second = load.run(engine, ["bitcoin"], from_raw=True, today=date(2026, 10, 13))

    assert (first["inserted"], first["updated"]) == (5, 0)
    assert (second["inserted"], second["updated"]) == (0, 5)
    assert load.table_count(engine) == 5
    with engine.connect() as conn:
        runs = conn.execute(select(db.pipeline_runs).order_by(db.pipeline_runs.c.run_id)).all()
    assert [r.status for r in runs] == ["success", "success"]
    assert all(r.finished_at is not None for r in runs)


def test_failed_run_is_recorded(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)  # empty folder: no raw files
    with pytest.raises(FileNotFoundError):
        load.run(engine, ["bitcoin"], from_raw=True)
    with engine.connect() as conn:
        run = conn.execute(select(db.pipeline_runs)).one()
    assert run.status == "failed"
    assert "no raw files for bitcoin" in run.error
