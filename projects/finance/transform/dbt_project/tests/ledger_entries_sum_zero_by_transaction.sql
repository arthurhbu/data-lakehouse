select
    transaction_id,
    sum(amount) as unbalanced_amount
from {{ ref('stg_ledger_entries') }}
group by transaction_id
having sum(amount) <> 0
