select
    ledger.entry_id,
    ledger.account_id
from {{ ref('stg_ledger_entries') }} as ledger
left join {{ ref('dim_accounts') }} as accounts
    on ledger.account_id = accounts.account_id
where accounts.account_id is null
