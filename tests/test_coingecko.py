import pytest
import requests

from crypto_pipeline.coingecko import CoinGeckoClient, CoinGeckoError, PermanentCoinGeckoError


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


class FakeSession:
    """Stands in for requests.Session: returns queued responses, records calls."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_client(responses):
    session = FakeSession(responses)
    return CoinGeckoClient("test-key", session=session, backoff_seconds=0), session


def test_success_sends_key_and_params(btc_payload):
    client, session = make_client([FakeResponse(200, btc_payload)])
    assert client.market_chart("bitcoin", "gbp", 365) == btc_payload
    url, params = session.calls[0]
    assert url.endswith("/coins/bitcoin/market_chart")
    assert params == {"vs_currency": "gbp", "days": 365, "interval": "daily"}
    assert session.headers["x-cg-demo-api-key"] == "test-key"


def test_retries_after_rate_limit_and_network_error(btc_payload):
    client, session = make_client(
        [FakeResponse(429), requests.ConnectionError("blip"), FakeResponse(200, btc_payload)]
    )
    assert client.market_chart("bitcoin", "gbp", 365) == btc_payload
    assert len(session.calls) == 3


def test_gives_up_after_max_retries():
    client, session = make_client([FakeResponse(500)] * 3)
    with pytest.raises(CoinGeckoError, match="gave up after 3 attempts"):
        client.market_chart("bitcoin", "gbp", 365)
    assert len(session.calls) == 3


def test_bad_key_fails_immediately():
    client, session = make_client([FakeResponse(401, {"error": "invalid key"})])
    with pytest.raises(PermanentCoinGeckoError, match="check your API key"):
        client.market_chart("bitcoin", "gbp", 365)
    assert len(session.calls) == 1  # no pointless retries


def test_other_client_errors_are_permanent():
    client, session = make_client([FakeResponse(400, {"error": "invalid key"})])
    with pytest.raises(PermanentCoinGeckoError, match="400"):
        client.market_chart("bitcoin", "gbp", 365)
    assert len(session.calls) == 1
