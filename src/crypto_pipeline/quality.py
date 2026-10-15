"""Milestone 3: data-quality checks.

Two layers:
  * batch checks (Pandera) run on freshly extracted rows BEFORE loading. If any
    fail, nothing is loaded, so bad data never reaches the database.
  * table checks (SQL) run on the whole table AFTER loading. They catch problems
    no single batch can show: missing days, stale data, a coin that stopped loading.

Every result is saved in the quality_results table.

Run the table checks on their own with:
    python -m crypto_pipeline.quality
    python -m crypto_pipeline.quality --today 2026-10-20   (pretend it's a later date)
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import insert, text
from sqlalchemy.engine import Engine

from crypto_pipeline import config, db
from crypto_pipeline.models import DailyPrice

MAX_DAILY_MOVE = 0.5  # a coin moving more than 50% in one day is almost surely bad data
MAX_STALENESS_DAYS = 1  # newest day must be today or yesterday (UTC)


@dataclass
class CheckResult:
    stage: str  # "batch" or "table"
    name: str
    passed: bool
    details: str


class DataQualityError(RuntimeError):
    pass


# ---------- batch checks (before loading) ----------


def daily_move_ok(df: pd.DataFrame) -> pd.Series:
    """True for each row whose price is within ±50% of that coin's previous day."""
    ordered = df.sort_values(["coin_id", "day"])
    move = ordered.groupby("coin_id")["price_gbp"].pct_change().abs()
    return (move.isna() | (move <= MAX_DAILY_MOVE)).reindex(df.index)


batch_schema = pa.DataFrameSchema(
    {
        "coin_id": pa.Column(str, pa.Check.isin(list(config.COINS), error="known_coin")),
        "day": pa.Column(pa.DateTime, coerce=True),
        "price_gbp": pa.Column(float, pa.Check.gt(0, error="price_positive")),
        "market_cap_gbp": pa.Column(float, pa.Check.ge(0, error="market_cap_not_negative")),
        "volume_gbp": pa.Column(float, pa.Check.ge(0, error="volume_not_negative")),
    },
    unique=["coin_id", "day"],  # one row per coin per day
    checks=[pa.Check(daily_move_ok, name="daily_move_under_50pct")],
    strict=True,  # no unexpected columns
)


def rows_to_frame(rows: list[DailyPrice]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "coin_id": [r.coin_id for r in rows],
            "day": [r.day for r in rows],
            "price_gbp": [r.price for r in rows],
            "market_cap_gbp": [r.market_cap for r in rows],
            "volume_gbp": [r.volume for r in rows],
        }
    )


def check_batch(df: pd.DataFrame) -> list[CheckResult]:
    """Validate a batch with Pandera. lazy=True collects every failure, not just the first."""
    try:
        batch_schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        cases_all = exc.failure_cases.copy()
        # Pandera's own name for the `unique=` rule, made readable
        cases_all["check"] = cases_all["check"].replace(
            {"multiple_fields_uniqueness": "one_row_per_coin_day"}
        )
        results = []
        for check, cases in cases_all.groupby("check", sort=True):
            # a whole-table check reports each bad row once per column, so de-duplicate rows
            cases = (
                cases.drop_duplicates(subset=["index"]) if cases["index"].notna().all() else cases
            )
            examples = []
            for idx, value in zip(cases["index"], cases["failure_case"]):
                if pd.notna(idx) and idx in df.index:
                    row = df.loc[idx]
                    examples.append(f"{row['coin_id']} {pd.Timestamp(row['day']).date()}")
                else:
                    examples.append(str(value))
            shown = ", ".join(examples[:3]) + (" ..." if len(examples) > 3 else "")
            results.append(CheckResult("batch", str(check), False, f"{len(cases)} bad: {shown}"))
        return results
    return [CheckResult("batch", "batch_schema", True, f"{len(df)} rows checked")]


# ---------- table checks (after loading) ----------


def check_table(engine: Engine, coins: list[str], today: date) -> list[CheckResult]:
    with engine.connect() as conn:
        stats = conn.execute(
            text(
                "SELECT coin_id, COUNT(*) AS n, MIN(day) AS first_day, MAX(day) AS last_day "
                "FROM raw_daily_prices GROUP BY coin_id"
            )
        ).all()
        missing = conn.execute(
            text(
                # every day between each coin's first and last day, minus the days we have
                "SELECT s.coin_id, s.day::date AS day FROM ("
                "  SELECT coin_id, generate_series(MIN(day), MAX(day), interval '1 day') AS day"
                "  FROM raw_daily_prices GROUP BY coin_id) s "
                "LEFT JOIN raw_daily_prices p ON p.coin_id = s.coin_id AND p.day = s.day "
                "WHERE p.day IS NULL ORDER BY s.coin_id, s.day"
            )
        ).all()
    by_coin = {s.coin_id: s for s in stats}

    absent = [c for c in coins if c not in by_coin]
    present = CheckResult(
        "table",
        "all_coins_present",
        not absent,
        f"missing coins: {', '.join(absent)}" if absent else f"{len(coins)} coins present",
    )

    gaps = [m for m in missing if m.coin_id in coins]
    no_gaps = CheckResult(
        "table",
        "no_missing_days",
        not gaps,
        (
            f"{len(gaps)} missing: "
            + ", ".join(f"{m.coin_id} {m.day}" for m in gaps[:5])
            + (" ..." if len(gaps) > 5 else "")
        )
        if gaps
        else "no gaps",
    )

    oldest_ok = today - timedelta(days=MAX_STALENESS_DAYS)
    stale = [
        f"{c} last={by_coin[c].last_day}"
        for c in coins
        if c in by_coin and by_coin[c].last_day < oldest_ok
    ]
    fresh = CheckResult(
        "table",
        "data_is_fresh",
        not stale,
        f"stale (need >= {oldest_ok}): {', '.join(stale)}" if stale else f"all >= {oldest_ok}",
    )
    return [present, no_gaps, fresh]


# ---------- recording ----------


def record(engine: Engine, run_id: int | None, results: list[CheckResult]) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(db.quality_results),
            [
                {
                    "run_id": run_id,
                    "stage": r.stage,
                    "check_name": r.name,
                    "passed": r.passed,
                    "details": r.details,
                }
                for r in results
            ],
        )


def raise_if_failed(results: list[CheckResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        summary = "; ".join(f"{r.name}: {r.details}" for r in failed)
        raise DataQualityError(f"{len(failed)} {failed[0].stage} check(s) failed: {summary}")


def print_report(results: list[CheckResult]) -> None:
    for r in results:
        print(f"  {'PASS' if r.passed else 'FAIL'}  {r.stage:<5}  {r.name:<24} {r.details}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the table-level data-quality checks")
    parser.add_argument("--today", type=date.fromisoformat, help="pretend today is YYYY-MM-DD")
    args = parser.parse_args()

    engine = db.get_engine()
    db.create_tables(engine)
    today = args.today or datetime.now(timezone.utc).date()
    results = check_table(engine, list(config.COINS), today)
    record(engine, None, results)
    print_report(results)
    sys.exit(0 if all(r.passed for r in results) else 1)


if __name__ == "__main__":
    main()
