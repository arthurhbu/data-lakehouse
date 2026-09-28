{{ config(materialized='table')}}

-- MVP: volume bruto de transações atualmente concluídas,
-- agrupado pela data de criação e moeda original.
-- Quando existirem eventos settled, a data deverá migrar para settlement date.

with completed_transactions as  (
    select
        cast(created_at as date) as transaction_date,
        transaction_id,
        currency as currency_code,
        amount
    from {{ ref('stg_transactions') }}
    where status = 'completed'
)
select
    transaction_date,
    currency_code,
    count(distinct transaction_id) as transaction_count,
    sum(amount) as gross_volume
from completed_transactions
group by transaction_date, currency_code