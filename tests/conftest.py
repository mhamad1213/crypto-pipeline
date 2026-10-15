import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from crypto_pipeline import db

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def btc_payload() -> dict:
    """A real-shaped CoinGecko response: 5 midnight points + one 'right now' point."""
    return json.loads((FIXTURES / "bitcoin_market_chart.json").read_text())


@pytest.fixture
def engine():
    """A real Postgres connection inside a throwaway schema.

    Every test gets its own empty schema (like a private folder in the database),
    which is deleted afterwards, so tests never touch your real data.
    Skips the test if Postgres isn't running.
    """
    schema = f"test_{uuid.uuid4().hex[:8]}"
    admin = db.get_engine()
    try:
        with admin.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {schema}"))
    except OperationalError:
        pytest.skip("Postgres is not running: start it with `docker compose up -d`")

    eng = db.get_engine(connect_args={"options": f"-csearch_path={schema}"})
    yield eng
    eng.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()
