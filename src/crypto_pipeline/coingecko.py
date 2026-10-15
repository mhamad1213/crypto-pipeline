"""A small CoinGecko client with retries and polite rate-limit handling."""

import time

import requests

from crypto_pipeline import config


class CoinGeckoError(RuntimeError):
    pass


class PermanentCoinGeckoError(CoinGeckoError):
    """An error that retrying won't fix, like a wrong API key or an unknown coin."""


class CoinGeckoClient:
    def __init__(
        self,
        api_key: str,
        session: requests.Session | None = None,
        max_retries: int = 3,
        backoff_seconds: float = 2.0,
    ):
        self.session = session or requests.Session()
        self.session.headers.update({"x-cg-demo-api-key": api_key, "accept": "application/json"})
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def _get(self, path: str, params: dict) -> dict:
        url = f"{config.BASE_URL}{path}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:  # network blip, DNS, timeout
                error = f"network error: {exc}"
            else:
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (401, 403):
                    raise PermanentCoinGeckoError(
                        f"{resp.status_code}: check your API key ({resp.text[:200]})"
                    )
                if resp.status_code == 404:
                    raise PermanentCoinGeckoError(f"404: unknown coin or path {path}")
                if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
                    # any other 4xx means our request is wrong: retrying won't help
                    raise PermanentCoinGeckoError(f"{resp.status_code}: {resp.text[:200]}")
                error = f"HTTP {resp.status_code}"  # 429 rate limit or 5xx: worth retrying
            if attempt < self.max_retries:
                wait = self.backoff_seconds * 2 ** (attempt - 1)  # 2s, 4s, 8s...
                print(f"  attempt {attempt} failed ({error}), retrying in {wait:.0f}s")
                time.sleep(wait)
        raise CoinGeckoError(f"gave up after {self.max_retries} attempts: {error}")

    def market_chart(self, coin_id: str, vs_currency: str, days: int) -> dict:
        """Daily price, market cap and volume history. Returns the raw JSON."""
        return self._get(
            f"/coins/{coin_id}/market_chart",
            {"vs_currency": vs_currency, "days": days, "interval": "daily"},
        )
