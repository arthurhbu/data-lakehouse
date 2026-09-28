{{ config(materialized='table') }}

select
    partner_id,
    name as partner_name,
    country as country_code, 
    partner_type, 
    status,
    created_at,
    updated_at
from {{ ref('stg_partners') }}