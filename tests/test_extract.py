import json

import pytest

from crypto_pipeline import config, extract


class FakeClient:
    def __init__(self, payload):
        self.payload = payload

    def market_chart(self, coin_id, vs_currency, days):
        return self.payload


def test_extract_coin_saves_raw_and_returns_rows(btc_payload, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    rows = extract.extract_coin(FakeClient(btc_payload), "bitcoin")

    saved = list((tmp_path / "bitcoin").glob("*.json"))
    assert len(saved) == 1
    assert json.loads(saved[0].read_text()) == btc_payload  # raw file is untouched
    assert len(rows) == 5 and all(r.coin_id == "bitcoin" for r in rows)


def test_rows_from_raw_uses_newest_file(btc_payload, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RAW_DIR", tmp_path)
    folder = tmp_path / "bitcoin"
    folder.mkdir()
    old = {**btc_payload, "prices": [[p[0], 1.0] for p in btc_payload["prices"]]}
    (folder / "2026-10-01T000000Z.json").write_text(json.dumps(old))
    (folder / "2026-10-14T000000Z.json").write_text(json.dumps(btc_payload))

    rows = extract.rows_from_raw("bitcoin")
    assert rows[-1].price == pytest.approx(93105.40)  # from the newer file


def test_missing_api_key_gives_clear_message(monkeypatch):
    monkeypatch.setenv("COINGECKO_API_KEY", "")
    with pytest.raises(RuntimeError, match="Copy .env.example to .env"):
        config.get_api_key()
