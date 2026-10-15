"""Milestone 5: the whole pipeline as one Prefect flow.

    extract (per coin, with retries) -> batch checks -> load -> table checks -> dbt build

Run with:  python -m crypto_pipeline.flow               (live API, then everything)
           python -m crypto_pipeline.flow --from-raw    (saved JSON instead of the API)
           python -m crypto_pipeline.flow --skip-dbt    (stop after loading)
"""

import argparse
import os
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy.engine import Engine

from crypto_pipeline import config, db, extract, load, quality
from crypto_pipeline.coingecko import CoinGeckoClient, PermanentCoinGeckoError
from crypto_pipeline.models import DailyPrice

# src/crypto_pipeline/flow.py -> project root -> transform/
TRANSFORM_DIR = Path(__file__).resolve().parents[2] / "transform"


class DbtBuildError(RuntimeError):
    pass


# NO_CACHE: always really run the task. Prefect can skip a task whose inputs it has
# seen before, which is useful elsewhere but wrong for "fetch today's prices".


def worth_retrying(task, task_run, state) -> bool:
    """Retry outages (rate limits, server errors, network). Don't retry a wrong
    API key or unknown coin: waiting won't fix those, so fail straight away."""
    try:
        state.result()
    except PermanentCoinGeckoError:
        return False
    except Exception:
        return True
    return True


@task(
    retries=2,
    retry_delay_seconds=[30, 120],
    retry_condition_fn=worth_retrying,
    cache_policy=NO_CACHE,
)
def fetch_coin(coin_id: str) -> list[DailyPrice]:
    """Call the API for one coin. If the whole call fails, Prefect tries again
    after 30s, then after 2 minutes, on top of the client's own quick retries."""
    client = CoinGeckoClient(config.get_api_key())
    return extract.extract_coin(client, coin_id)


@task(cache_policy=NO_CACHE)
def read_raw(coin_id: str) -> list[DailyPrice]:
    return extract.rows_from_raw(coin_id)


@task(cache_policy=NO_CACHE)
def check_batch(engine: Engine, run_id: int, rows: list[DailyPrice]) -> None:
    load.validate_batch(engine, run_id, rows)


@task(cache_policy=NO_CACHE)
def load_prices(engine: Engine, coins: list[str], rows: list[DailyPrice]) -> tuple[int, int]:
    return load.load_rows(engine, coins, rows)


@task(cache_policy=NO_CACHE)
def check_table(engine: Engine, run_id: int, coins: list[str], today: date) -> None:
    load.validate_table(engine, run_id, coins, today)


@task(cache_policy=NO_CACHE)
def dbt_build(engine: Engine, run_id: int) -> str:
    """Run `dbt build` (all models + tests) from Python and record the outcome."""
    os.environ.update(config.dbt_env())  # dbt connects to the same database as the load
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(TRANSFORM_DIR), "--profiles-dir", str(TRANSFORM_DIR)]
    )
    if result.exception is not None:  # dbt itself crashed, e.g. can't reach the database
        raise DbtBuildError(f"dbt could not run: {result.exception}")
    counts = Counter(str(r.status) for r in result.result.results)
    summary = ", ".join(f"{status}={n}" for status, n in sorted(counts.items()))
    quality.record(
        engine, run_id, [quality.CheckResult("dbt", "dbt_build", result.success, summary)]
    )
    if not result.success:
        raise DbtBuildError(f"dbt build failed: {summary}")
    return summary


@flow(name="daily-crypto-pipeline", log_prints=True)
def daily_pipeline(
    coins: list[str] | None = None,
    from_raw: bool = False,
    run_dbt: bool = True,
    today: date | None = None,
) -> dict:
    coins = coins or list(config.COINS)
    today = today or datetime.now(timezone.utc).date()
    engine = db.get_engine()

    run_id = load.start_run(engine, "raw" if from_raw else "api")
    inserted = updated = 0
    status, error = "failed", "interrupted"
    dbt_summary = "skipped"
    try:
        rows: list[DailyPrice] = []
        for i, coin_id in enumerate(coins):
            if from_raw:
                rows += read_raw(coin_id)
            else:
                if i:
                    time.sleep(2)  # stay well under the Demo plan's rate limit
                rows += fetch_coin(coin_id)

        check_batch(engine, run_id, rows)  # raises -> nothing loaded
        inserted, updated = load_prices(engine, coins, rows)
        check_table(engine, run_id, coins, today)  # raises -> run marked failed
        if run_dbt:
            dbt_summary = dbt_build(engine, run_id)
        status, error = "success", None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        load.finish_run(engine, run_id, status, inserted, updated, error)

    print(f"run {run_id}: inserted {inserted}, updated {updated}, dbt: {dbt_summary}")
    return {"run_id": run_id, "inserted": inserted, "updated": updated, "dbt": dbt_summary}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full daily crypto pipeline")
    parser.add_argument("--from-raw", action="store_true", help="use saved JSON, no API calls")
    parser.add_argument("--skip-dbt", action="store_true", help="stop after loading")
    parser.add_argument("coins", nargs="*", default=None)
    args = parser.parse_args()
    daily_pipeline(coins=args.coins or None, from_raw=args.from_raw, run_dbt=not args.skip_dbt)


if __name__ == "__main__":
    main()
