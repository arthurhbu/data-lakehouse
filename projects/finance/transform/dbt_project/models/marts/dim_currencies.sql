{{ config(materialized='table') }}

select distinct
    account_currency as currency_code
from {{ ref('dim_accounts') }}
where account_currency is not null