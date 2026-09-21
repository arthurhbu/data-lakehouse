{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'partners') }}

)
select
    partner_id,
    name,
    country,
    partner_type,
    status,
    created_at,
    updated_at
from source
where _cdc_deleted=false
