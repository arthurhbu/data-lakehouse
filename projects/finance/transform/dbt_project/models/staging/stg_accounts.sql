{{ config(materialized='table') }}

with source as (
    select * 
    from {{ source ('silver', 'accounts') }}

)
select
    account_id,
    partner_id,
    currency,
    account_type,
    status,
    created_at,
    updated_at
from source
where _cdc_deleted=false
