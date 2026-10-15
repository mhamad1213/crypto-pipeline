select
    coin_id,
    upper(symbol) as symbol,
    name as coin_name
from {{ source('raw', 'coins') }}
