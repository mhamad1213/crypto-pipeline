"""Pydantic models: check the API response looks right, then turn it into clean rows."""

from datetime import date, datetime, timezone

from pydantic import BaseModel, Field, field_validator, model_validator

# Each point from CoinGecko is [timestamp_in_milliseconds, value]
Point = tuple[int, float]


class MarketChart(BaseModel):
    """The raw shape of /coins/{id}/market_chart. Anything else fails loudly."""

    prices: list[Point] = Field(min_length=1)
    market_caps: list[Point]
    total_volumes: list[Point]

    @field_validator("prices")
    @classmethod
    def prices_positive(cls, points: list[Point]) -> list[Point]:
        for ts, price in points:
            if price <= 0:
                raise ValueError(f"non-positive price {price} at timestamp {ts}")
        return points

    @model_validator(mode="after")
    def same_length(self) -> "MarketChart":
        n = len(self.prices)
        if len(self.market_caps) != n or len(self.total_volumes) != n:
            raise ValueError(
                f"lists have different lengths: prices={n}, "
                f"market_caps={len(self.market_caps)}, total_volumes={len(self.total_volumes)}"
            )
        return self


class DailyPrice(BaseModel):
    """One clean row: one coin on one day."""

    coin_id: str
    day: date
    price: float = Field(gt=0)
    market_cap: float = Field(ge=0)
    volume: float = Field(ge=0)


MS_PER_DAY = 86_400_000


def to_daily_rows(coin_id: str, chart: MarketChart) -> list[DailyPrice]:
    """Convert the three parallel lists into one row per day.

    Each daily point from CoinGecko is a snapshot taken at exactly 00:00 UTC, so
    `day` means "the price at midnight UTC as this day started". The last point
    in a response is usually "right now" (e.g. 15:07), which isn't a daily
    snapshot, so anything not exactly on midnight is dropped. If the same
    midnight appears twice, the later point wins.
    """
    rows: dict[date, DailyPrice] = {}
    for (ts, price), (_, cap), (_, vol) in zip(
        chart.prices, chart.market_caps, chart.total_volumes
    ):
        if ts % MS_PER_DAY != 0:
            continue
        day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
        rows[day] = DailyPrice(coin_id=coin_id, day=day, price=price, market_cap=cap, volume=vol)
    return [rows[d] for d in sorted(rows)]
