with silver_expected as (

    select
        cast(created_at as date) as transaction_date,
        currency as currency_code,
        count(distinct transaction_id) as expected_transaction_count,
        sum(amount) as expected_gross_volume
    from {{ source('silver', 'transactions') }}
    where _cdc_deleted = false
      and status = 'completed'
    group by cast(created_at as date), currency

),

gold_actual as (

    select
        transaction_date,
        currency_code,
        sum(transaction_count) as actual_transaction_count,
        sum(gross_volume) as actual_gross_volume
    from {{ ref('fct_daily_volume') }}
    group by transaction_date, currency_code

)

select
    coalesce(s.transaction_date, g.transaction_date) as transaction_date,
    coalesce(s.currency_code, g.currency_code) as currency_code,
    s.expected_transaction_count,
    g.actual_transaction_count,
    s.expected_gross_volume,
    g.actual_gross_volume
from silver_expected s
full outer join gold_actual g
    on s.transaction_date = g.transaction_date
    and s.currency_code = g.currency_code
where coalesce(s.expected_transaction_count, 0)
        <> coalesce(g.actual_transaction_count, 0)
   or coalesce(s.expected_gross_volume, 0)
        <> coalesce(g.actual_gross_volume, 0)
