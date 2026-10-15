-- One row per coin: the "headline numbers" for the dashboard.

with metrics as (
    select * from {{ ref('fct_daily_metrics') }}
),

latest as (
    select distinct on (coin_id) *
    from metrics
    order by coin_id, price_date desc
),

history as (
    select
        coin_id,
        count(*) as days_of_data,
        min(price_date) as first_date,
        min(drawdown) as max_drawdown
    from metrics
    group by coin_id
)

select
    l.coin_id,
    l.symbol,
    l.price_date as latest_date,
    l.price_gbp as latest_price_gbp,
    l.price_gbp / p7.price_gbp - 1 as return_7d,
    l.price_gbp / p30.price_gbp - 1 as return_30d,
    l.volatility_30d_annualised,
    l.drawdown as current_drawdown,
    h.max_drawdown,
    h.days_of_data,
    h.first_date
from latest l
join history h using (coin_id)
left join metrics p7 on p7.coin_id = l.coin_id and p7.price_date = l.price_date - 7
left join metrics p30 on p30.coin_id = l.coin_id and p30.price_date = l.price_date - 30
