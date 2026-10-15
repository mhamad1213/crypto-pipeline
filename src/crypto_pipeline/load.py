"""Extract each coin, check it, load it into Postgres, check the table, record the run.

The steps are small functions so the Prefect flow (flow.py) can reuse them.
This file still works on its own, without Prefect or dbt:

Run with:  python -m crypto_pipeline.load              (call the API, then load)
           python -m crypto_pipeline.load --from-raw   (re-load the newest saved JSON, no API calls)
"""

import argparse
import time
from datetime import date, datetime, timezone

from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import Engine

from crypto_pipeline import config, db, extract, quality
from crypto_pipeline.coingecko import CoinGeckoClient
from crypto_pipeline.models import DailyPrice


def start_run(engine: Engine, source: str) -> int:
    """Create the tables if needed and log a 'running' row. Returns the new run_id."""
    db.create_tables(engine)
    with engine.begin() as conn:
        return conn.execute(
            insert(db.pipeline_runs)
            .values(status="running", source=source)
            .returning(db.pipeline_runs.c.run_id)
        ).scalar_one()


def finish_run(
    engine: Engine, run_id: int, status: str, inserted: int, updated: int, error: str | None
) -> None:
    with engine.begin() as conn:
        conn.execute(
            update(db.pipeline_runs)
            .where(db.pipeline_runs.c.run_id == run_id)
            .values(
                status=status,
                finished_at=func.now(),
                rows_inserted=inserted,
                rows_updated=updated,
                error=error,
            )
        )


def extract_all(coins: list[str], from_raw: bool) -> list[DailyPrice]:
    if from_raw:
        return [row for coin_id in coins for row in extract.rows_from_raw(coin_id)]
    client = CoinGeckoClient(config.get_api_key())
    rows = []
    for i, coin_id in enumerate(coins):
        if i:
            time.sleep(2)  # stay well under the Demo plan's rate limit
        rows += extract.extract_coin(client, coin_id)
    return rows


def validate_batch(engine: Engine, run_id: int, rows: list[DailyPrice]) -> None:
    """Batch checks: if any fail, raise before anything is loaded."""
    results = quality.check_batch(quality.rows_to_frame(rows))
    quality.record(engine, run_id, results)
    quality.print_report(results)
    quality.raise_if_failed(results)


def load_rows(engine: Engine, coins: list[str], rows: list[DailyPrice]) -> tuple[int, int]:
    inserted = updated = 0
    for coin_id in coins:
        ins, upd = db.upsert_prices(engine, [r for r in rows if r.coin_id == coin_id])
        inserted, updated = inserted + ins, updated + upd
        print(f"  loaded {coin_id:<9} inserted={ins:<4} updated={upd}")
    return inserted, updated


def validate_table(engine: Engine, run_id: int, coins: list[str], today: date) -> None:
    """Table checks on everything now in the database."""
    results = quality.check_table(engine, coins, today)
    quality.record(engine, run_id, results)
    quality.print_report(results)
    quality.raise_if_failed(results)


def run(
    engine: Engine, coins: list[str], from_raw: bool = False, today: date | None = None
) -> dict:
    today = today or datetime.now(timezone.utc).date()
    run_id = start_run(engine, "raw" if from_raw else "api")
    inserted = updated = 0
    status, error = "failed", "interrupted"  # overwritten below unless the run is cut short
    try:
        rows = extract_all(coins, from_raw)
        validate_batch(engine, run_id, rows)
        inserted, updated = load_rows(engine, coins, rows)
        validate_table(engine, run_id, coins, today)
        status, error = "success", None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # Runs whether the try block succeeded or crashed, so every run is recorded.
        finish_run(engine, run_id, status, inserted, updated, error)
    return {"run_id": run_id, "inserted": inserted, "updated": updated}


def table_count(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(db.raw_daily_prices)).scalar_one()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load daily crypto prices into Postgres")
    parser.add_argument("--from-raw", action="store_true", help="use saved JSON, no API calls")
    parser.add_argument("coins", nargs="*", default=list(config.COINS))
    args = parser.parse_args()

    engine = db.get_engine()
    result = run(engine, args.coins, from_raw=args.from_raw)
    print(
        f"run {result['run_id']}: inserted {result['inserted']}, updated {result['updated']}. "
        f"raw_daily_prices now has {table_count(engine)} rows"
    )


if __name__ == "__main__":
    main()
