{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'transactions') }}

)

select
    transaction_id,
    source_account_id,
    destination_account_id,
    amount,
    currency,
    fx_rate,
    converted_amount,
    converted_currency,
    payment_method,
    status,
    description,
    created_at,
    updated_at
from source
where _cdc_deleted=false
