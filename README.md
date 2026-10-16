# Daily Crypto Data Pipeline

[![CI](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/ci.yml)
[![Daily pipeline](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/daily.yml/badge.svg)](https://github.com/mhamad1213/crypto-pipeline/actions/workflows/daily.yml)

Every morning a GitHub Actions job fetches daily prices for 5 cryptocurrencies from the
CoinGecko API, validates them (Pydantic + Pandera), loads them into PostgreSQL (Neon),
transforms them with dbt (returns, rolling volatility, drawdown), forecasts each coin's
volatility for the next 7 days, and logs every run and quality check. The whole run is a
Prefect flow with retries.

**Live dashboard:** https://mhamad-crypto-pipeline.streamlit.app (Streamlit, reading Neon through a read-only login)

**Forecast:** a HAR-style Ridge regression predicts next-week volatility and is scored
every day against a naive baseline ("same as last week") with a walk-forward test, so the
model is only ever trained on the past. Full write-up to follow.

## Run it locally

```bash
docker compose up -d                      # local Postgres
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r dashboard/requirements.txt && pip install -e .
cp .env.example .env                      # then paste your CoinGecko Demo key
python -m pytest                          # 61 tests
python -m crypto_pipeline.flow            # fetch -> check -> load -> dbt -> forecast
python -m crypto_pipeline.forecast        # just the model vs baseline report
streamlit run dashboard/app.py            # dashboard on http://localhost:8501
```
