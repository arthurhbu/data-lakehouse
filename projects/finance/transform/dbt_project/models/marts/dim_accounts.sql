{{ config(materialized='table') }}

select 
    account_id,
    partner_id,
    currency as account_currency,
    account_type,
    status,
    created_at,
    updated_at
from {{ ref('stg_accounts') }}