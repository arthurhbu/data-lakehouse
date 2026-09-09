{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'payment_events') }}

)

select * 
from source
where _cdc_deleted=false