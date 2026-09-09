{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'partners') }}

)

select * 
from source
where _cdc_deleted=false