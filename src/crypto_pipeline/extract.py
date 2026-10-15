"""Extract: fetch daily history for each coin, save the raw JSON, validate it.

Run with:  python -m crypto_pipeline.extract            (all coins)
           python -m crypto_pipeline.extract bitcoin    (one coin)
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from crypto_pipeline import config
from crypto_pipeline.coingecko import CoinGeckoClient
from crypto_pipeline.models import DailyPrice, MarketChart, to_daily_rows


def save_raw(coin_id: str, payload: dict, raw_dir: Path | None = None) -> Path:
    """Keep the untouched API response, so any run can be replayed later."""
    raw_dir = raw_dir or config.RAW_DIR
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    path = raw_dir / coin_id / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


def parse(coin_id: str, payload: dict) -> list[DailyPrice]:
    chart = MarketChart.model_validate(payload)  # raises if the shape is wrong
    return to_daily_rows(coin_id, chart)


def latest_raw_file(coin_id: str) -> Path:
    files = sorted((config.RAW_DIR / coin_id).glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no raw files for {coin_id} in {config.RAW_DIR / coin_id}")
    return files[-1]  # names are timestamps, so the last one is the newest


def rows_from_raw(coin_id: str) -> list[DailyPrice]:
    """Re-process the newest saved response without calling the API."""
    return parse(coin_id, json.loads(latest_raw_file(coin_id).read_text()))


def extract_coin(client: CoinGeckoClient, coin_id: str) -> list[DailyPrice]:
    payload = client.market_chart(coin_id, config.VS_CURRENCY, config.HISTORY_DAYS)
    path = save_raw(coin_id, payload)
    rows = parse(coin_id, payload)
    first, last = rows[0], rows[-1]
    print(
        f"{coin_id:<9} {len(rows)} days  {first.day} -> {last.day}  "
        f"latest 00:00 UTC price £{last.price:,.2f}  saved {path}"
    )
    return rows


def main(coins: list[str]) -> None:
    client = CoinGeckoClient(config.get_api_key())
    for i, coin_id in enumerate(coins):
        if i:
            time.sleep(2)  # stay well under the Demo plan's rate limit
        extract_coin(client, coin_id)


if __name__ == "__main__":
    main(sys.argv[1:] or list(config.COINS))
