-- A one-off ("singular") test: any row returned is a failure.
-- Each coin must have exactly one row for every day between its first and last date.
select
    coin_id,
    count(*) as rows_found,
    max(price_date) - min(price_date) + 1 as days_expected
from {{ ref('fct_daily_metrics') }}
group by coin_id
having count(*) <> max(price_date) - min(price_date) + 1
