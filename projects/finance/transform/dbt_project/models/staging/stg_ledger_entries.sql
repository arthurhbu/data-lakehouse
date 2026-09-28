{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'ledger_entries') }}

)

select
    entry_id,
    transaction_id,
    account_id,
    entry_type,
    amount,
    currency,
    description,
    created_at
from source
where _cdc_deleted=false
