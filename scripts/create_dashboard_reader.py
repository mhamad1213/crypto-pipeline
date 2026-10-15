"""Create (or reset) a read-only database login for the public dashboard.

The dashboard only needs to *read*. Giving it the owner's password would let anyone who
found a bug in it change or delete your data, so it gets its own login that can only SELECT.

Usage (run once, against the database DATABASE_URL points at):
    DATABASE_URL="$(cat .neon-url)" python scripts/create_dashboard_reader.py

Writes the reader's connection string to .neon-reader-url (git-ignored).
"""

import secrets
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError

from crypto_pipeline import config

ROLE = "dashboard_reader"
OUT = Path(".neon-reader-url")


def main() -> None:
    owner_url = make_url(config.get_database_url())
    owner = owner_url.username
    password = secrets.token_urlsafe(24)  # 192 random bits; Neon requires a strong one

    with create_engine(owner_url).begin() as conn:
        find = text("SELECT 1 FROM pg_roles WHERE rolname = :r")
        exists = conn.execute(find, {"r": ROLE}).first() is not None
        verb = "ALTER" if exists else "CREATE"
        # A password can't be a query parameter in CREATE ROLE, so it's quoted safely here.
        quoted = conn.execute(text("SELECT quote_literal(:p)"), {"p": password}).scalar_one()
        conn.execute(text(f"{verb} ROLE {ROLE} WITH LOGIN PASSWORD {quoted}"))
        conn.execute(text(f"GRANT CONNECT ON DATABASE {owner_url.database} TO {ROLE}"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        for schema in ("public", "analytics"):
            conn.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {ROLE}"))
            conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {ROLE}"))
            # dbt rebuilds its tables every run; this makes every new table readable too.
            conn.execute(
                text(
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                    f"GRANT SELECT ON TABLES TO {ROLE}"
                )
            )
    done = "reset the password of" if exists else "created"
    print(f"{done} role {ROLE}: SELECT only, on schemas public and analytics")

    reader_url = owner_url.set(username=ROLE, password=password)
    OUT.write_text(reader_url.render_as_string(hide_password=False) + "\n")
    print(f"saved its connection string to {OUT}")

    # Prove it: the reader can read, but not write.
    with create_engine(reader_url).connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM raw_daily_prices")).scalar_one()
        print(f"reader can SELECT: raw_daily_prices has {rows:,} rows")
        try:
            conn.execute(text("DELETE FROM raw_daily_prices"))
            print("WARNING: reader could DELETE. Something is wrong; don't use this login.")
        except ProgrammingError as exc:
            print(f"reader cannot DELETE: {exc.orig.diag.message_primary}")


if __name__ == "__main__":
    main()
