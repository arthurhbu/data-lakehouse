{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('silver', 'ledger_entries') }}

)

select * 
from source
where _cdc_deleted=false