-- One row per coin per day, with everything the dashboard and the model need.
--
-- Window functions do the work: for each row they look at other rows of the
-- same coin (partition by coin_id), in date order (order by price_date).

with prices as (
    select * from {{ ref('stg_daily_prices') }}
),

with_returns as (
    select
        *,
        lag(price_gbp) over w as prev_price_gbp,
        price_gbp / lag(price_gbp) over w - 1 as daily_return,
        ln(price_gbp / lag(price_gbp) over w) as log_return
    from prices
    window w as (partition by coin_id order by price_date)
),

with_windows as (
    select
        *,
        -- moving averages: only filled once a full 7 / 30 days exist
        case when count(*) over w7 = 7 then avg(price_gbp) over w7 end as ma_7d,
        case when count(*) over w30 = 30 then avg(price_gbp) over w30 end as ma_30d,
        -- volatility = how much daily returns jump around (standard deviation)
        case when count(log_return) over w7 = 7 then stddev_samp(log_return) over w7 end
            as volatility_7d,
        case when count(log_return) over w30 = 30 then stddev_samp(log_return) over w30 end
            as volatility_30d,
        -- highest price so far, for drawdown
        max(price_gbp) over (
            partition by coin_id order by price_date
            rows between unbounded preceding and current row
        ) as running_max_gbp
    from with_returns
    window
        w7 as (partition by coin_id order by price_date rows between 6 preceding and current row),
        w30 as (partition by coin_id order by price_date rows between 29 preceding and current row)
)

select
    m.coin_id || '|' || m.price_date as coin_day_key,
    m.coin_id,
    c.symbol,
    m.price_date,
    m.price_gbp,
    m.market_cap_gbp,
    m.volume_gbp,
    m.daily_return,
    m.log_return,
    m.ma_7d,
    m.ma_30d,
    m.volatility_7d,
    m.volatility_30d,
    m.volatility_30d * sqrt(365) as volatility_30d_annualised,  -- crypto trades every day
    m.running_max_gbp,
    m.price_gbp / m.running_max_gbp - 1 as drawdown  -- 0 = at the peak, -0.4 = 40% below it
from with_windows m
join {{ ref('stg_coins') }} c using (coin_id)
