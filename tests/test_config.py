import pytest

from crypto_pipeline import config

NEON = "postgresql://alice:s3cret@ep-cool-1234.eu-west-2.aws.neon.tech/neondb?sslmode=require"


@pytest.mark.parametrize(
    "given, expected",
    [
        ("", config.DEFAULT_DATABASE_URL),
        ("postgres://u:p@host/db", "postgresql+psycopg://u:p@host/db"),
        ("postgresql://u:p@host/db", "postgresql+psycopg://u:p@host/db"),
        ("postgresql+psycopg://u:p@host/db", "postgresql+psycopg://u:p@host/db"),
    ],
)
def test_database_url_gets_psycopg_driver(monkeypatch, given, expected):
    monkeypatch.setenv("DATABASE_URL", given)
    assert config.get_database_url() == expected


def test_dbt_env_from_neon_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", NEON)
    assert config.dbt_env() == {
        "DBT_HOST": "ep-cool-1234.eu-west-2.aws.neon.tech",
        "DBT_PORT": "5432",
        "DBT_USER": "alice",
        "DBT_PASSWORD": "s3cret",
        "DBT_DBNAME": "neondb",
        "DBT_SSLMODE": "require",
    }


def test_dbt_env_defaults_to_local_docker(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    env = config.dbt_env()
    assert env["DBT_HOST"] == "localhost"
    assert (env["DBT_USER"], env["DBT_DBNAME"]) == ("crypto", "crypto")
    assert env["DBT_SSLMODE"] == "prefer"


def test_dbt_env_decodes_special_characters_in_password(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://bob:p%40ss%2Fword@host:6543/db")
    env = config.dbt_env()
    assert (env["DBT_PASSWORD"], env["DBT_PORT"]) == ("p@ss/word", "6543")
