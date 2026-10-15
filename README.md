# Daily Crypto Data Pipeline

[![CI](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/ci.yml)
[![Daily pipeline](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/daily.yml/badge.svg)](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/daily.yml)

Every morning a GitHub Actions job fetches daily prices for 5 cryptocurrencies from the
CoinGecko API, validates them (Pydantic + Pandera), loads them into PostgreSQL (Neon),
transforms them with dbt (returns, rolling volatility, drawdown) and logs every run and
quality check. The whole run is a Prefect flow with retries.

Work in progress: dashboard and volatility forecast are next. Full write-up to follow.

## Run it locally

```bash
docker compose up -d                      # local Postgres
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
cp .env.example .env                      # then paste your CoinGecko Demo key
python -m pytest                          # 45 tests
python -m crypto_pipeline.flow            # fetch -> check -> load -> dbt build
```
