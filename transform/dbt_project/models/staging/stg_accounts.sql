{{ config(materialized='table') }}

with source as (
    select * 
    from {{ source ('silver', 'accounts') }}

)
select *
from source
where _cdc_deleted=false

