select
    partner_id,
    currency_code,
    count(*) as row_count
from {{ ref('fct_partner_net_position') }}
group by partner_id, currency_code
having count(*) > 1
