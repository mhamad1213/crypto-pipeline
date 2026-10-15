-- Sanity check on the window maths: a 7-day average must lie between that
-- window's lowest and highest price. If not, the window is wrong.
with w as (
    select
        coin_id,
        price_date,
        ma_7d,
        min(price_gbp) over w7 as low_7d,
        max(price_gbp) over w7 as high_7d
    from {{ ref('fct_daily_metrics') }}
    window w7 as (partition by coin_id order by price_date rows between 6 preceding and current row)
)
select *
from w
where ma_7d is not null
  and (ma_7d < low_7d - 1e-9 or ma_7d > high_7d + 1e-9)
