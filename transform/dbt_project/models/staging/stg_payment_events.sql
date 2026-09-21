{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'payment_events') }}

)
select
    event_id,
    transaction_id,
    event_type,
    metadata,
    created_at
from source
where _cdc_deleted=false
