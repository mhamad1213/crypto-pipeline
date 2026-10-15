"""Database tables and the idempotent upsert."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    literal_column,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from crypto_pipeline import config
from crypto_pipeline.models import DailyPrice

metadata = MetaData()

coins = Table(
    "coins",
    metadata,
    Column("coin_id", String(50), primary_key=True),
    Column("symbol", String(10), nullable=False),
    Column("name", String(100), nullable=False),
)

raw_daily_prices = Table(
    "raw_daily_prices",
    metadata,
    Column("coin_id", String(50), ForeignKey("coins.coin_id"), primary_key=True),
    Column("day", Date, primary_key=True),  # (coin_id, day) together are unique
    Column("price_gbp", Float, nullable=False),
    Column("market_cap_gbp", Float, nullable=False),
    Column("volume_gbp", Float, nullable=False),
    Column("loaded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(20), nullable=False),  # running / success / failed
    Column("source", String(20), nullable=False),  # api / raw
    Column("rows_inserted", BigInteger),
    Column("rows_updated", BigInteger),
    Column("error", Text),
)

quality_results = Table(
    "quality_results",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("pipeline_runs.run_id")),  # empty for standalone checks
    Column("checked_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("stage", String(10), nullable=False),  # batch / table
    Column("check_name", String(50), nullable=False),
    Column("passed", Boolean, nullable=False),
    Column("details", Text),
)


def get_engine(url: str | None = None, **kwargs) -> Engine:
    return create_engine(url or config.get_database_url(), **kwargs)


def create_tables(engine: Engine) -> None:
    """Create any missing tables (safe to run every time) and fill in the coins list."""
    metadata.create_all(engine)
    stmt = insert(coins).values(
        [{"coin_id": cid, "symbol": sym, "name": name} for cid, (sym, name) in config.COINS.items()]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["coin_id"],
        set_={"symbol": stmt.excluded.symbol, "name": stmt.excluded.name},
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def upsert_prices(engine: Engine, rows: list[DailyPrice]) -> tuple[int, int]:
    """Insert new (coin, day) rows and update existing ones. Returns (inserted, updated).

    Running this twice with the same rows never creates duplicates: the second
    time every row hits ON CONFLICT and becomes an update instead.
    """
    if not rows:
        return 0, 0
    stmt = insert(raw_daily_prices).values(
        [
            {
                "coin_id": r.coin_id,
                "day": r.day,
                "price_gbp": r.price,
                "market_cap_gbp": r.market_cap,
                "volume_gbp": r.volume,
            }
            for r in rows
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["coin_id", "day"],
        set_={
            "price_gbp": stmt.excluded.price_gbp,
            "market_cap_gbp": stmt.excluded.market_cap_gbp,
            "volume_gbp": stmt.excluded.volume_gbp,
            "loaded_at": func.now(),
        },
    ).returning(literal_column("(xmax = 0)").label("inserted"))  # Postgres: true if new row
    with engine.begin() as conn:
        flags = [row.inserted for row in conn.execute(stmt)]
    inserted = sum(flags)
    return inserted, len(flags) - inserted
