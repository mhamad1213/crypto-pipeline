from datetime import date

import pytest
from pydantic import ValidationError

from crypto_pipeline.models import MarketChart, to_daily_rows


def test_valid_payload_parses(btc_payload):
    chart = MarketChart.model_validate(btc_payload)
    assert len(chart.prices) == 6


def test_only_midnight_snapshots_kept(btc_payload):
    # 5 midnight points + one "right now" point at 14:32 -> 5 rows
    chart = MarketChart.model_validate(btc_payload)
    rows = to_daily_rows("bitcoin", chart)
    assert [r.day for r in rows] == [date(2026, 10, d) for d in (9, 10, 11, 12, 13)]
    assert rows[-1].price == pytest.approx(93105.40)  # the 00:00 price, not the 14:32 one


def test_duplicate_midnight_keeps_later_point(btc_payload):
    for key, value in [("prices", 1.0), ("market_caps", 2.0), ("total_volumes", 3.0)]:
        btc_payload[key].append([btc_payload[key][0][0], value])  # repeat 9 Oct midnight
    chart = MarketChart.model_validate(btc_payload)
    rows = to_daily_rows("bitcoin", chart)
    assert len(rows) == 5
    assert rows[0].price == 1.0


def test_negative_price_rejected(btc_payload):
    btc_payload["prices"][2][1] = -5
    with pytest.raises(ValidationError, match="non-positive price"):
        MarketChart.model_validate(btc_payload)


def test_mismatched_lengths_rejected(btc_payload):
    btc_payload["total_volumes"].pop()
    with pytest.raises(ValidationError, match="different lengths"):
        MarketChart.model_validate(btc_payload)


def test_missing_field_rejected(btc_payload):
    del btc_payload["market_caps"]
    with pytest.raises(ValidationError):
        MarketChart.model_validate(btc_payload)
