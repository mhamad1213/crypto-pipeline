-- Clean names, nothing else. Every later model reads from here, never from the
-- raw table directly, so if the raw table ever changes only this file changes.
select
    coin_id,
    day as price_date,
    price_gbp,
    market_cap_gbp,
    volume_gbp
from {{ source('raw', 'raw_daily_prices') }}
