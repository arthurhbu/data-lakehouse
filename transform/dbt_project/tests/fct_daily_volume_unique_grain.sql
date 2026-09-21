select
    transaction_date,
    currency_code,
    count(*) as row_count
from {{ ref('fct_daily_volume') }}
group by transaction_date, currency_code
having count(*) > 1
