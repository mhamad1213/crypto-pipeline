"""Settings for the whole pipeline, in one place."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.engine import make_url

load_dotenv()  # reads .env in the folder you run from, if it exists

# CoinGecko id -> (ticker symbol, display name). The id is the last part of the coin's page URL.
COINS = {
    "bitcoin": ("BTC", "Bitcoin"),
    "ethereum": ("ETH", "Ethereum"),
    "solana": ("SOL", "Solana"),
    "ripple": ("XRP", "XRP"),
    "cardano": ("ADA", "Cardano"),
}
VS_CURRENCY = "gbp"
HISTORY_DAYS = 365

BASE_URL = "https://api.coingecko.com/api/v3"
RAW_DIR = Path("data/raw")

# Matches docker-compose.yml. Set DATABASE_URL (in .env or a GitHub secret) to use Neon instead.
DEFAULT_DATABASE_URL = "postgresql+psycopg://crypto:crypto@localhost:5432/crypto"


def get_api_key() -> str:
    key = os.getenv("COINGECKO_API_KEY", "").strip()
    if not key or key == "your-demo-key-here":
        raise RuntimeError(
            "COINGECKO_API_KEY is not set. Copy .env.example to .env and paste your Demo key."
        )
    return key


def get_database_url() -> str:
    """DATABASE_URL if set, else local Docker. Neon's postgresql:// form is accepted as is."""
    url = os.getenv("DATABASE_URL", "").strip() or DEFAULT_DATABASE_URL
    # Neon (and most hosts) hand out postgres:// or postgresql:// URLs. SQLAlchemy needs to be
    # told which driver to use, so swap the scheme for postgresql+psycopg://.
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def dbt_env() -> dict[str, str]:
    """Split DATABASE_URL into the DBT_* variables that transform/profiles.yml reads.

    This keeps one setting (DATABASE_URL) in charge of both Python and dbt.
    """
    url = make_url(get_database_url())
    sslmode = url.query.get("sslmode", "prefer")
    if isinstance(sslmode, tuple):  # make_url returns a tuple if the key appears twice
        sslmode = sslmode[-1]
    return {
        "DBT_HOST": url.host or "localhost",
        "DBT_PORT": str(url.port or 5432),
        "DBT_USER": url.username or "",
        "DBT_PASSWORD": url.password or "",
        "DBT_DBNAME": url.database or "",
        "DBT_SSLMODE": sslmode,
    }
