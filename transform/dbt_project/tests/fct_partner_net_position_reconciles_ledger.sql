with ledger_by_currency as (
    select
        currency as currency_code,
        count(*) as expected_entry_count,
        sum(case when entry_type = 'debit' then amount else 0 end)
            as expected_total_debits,
        sum(case when entry_type = 'credit' then -amount else 0 end)
            as expected_total_credits,
        -sum(amount) as expected_net_position
    from {{ ref('stg_ledger_entries') }}
    group by currency
),

fact_by_currency as (
    select
        currency_code,
        sum(entry_count) as actual_entry_count,
        sum(total_debits) as actual_total_debits,
        sum(total_credits) as actual_total_credits,
        sum(net_position) as actual_net_position
    from {{ ref('fct_partner_net_position') }}
    group by currency_code
)

select
    coalesce(ledger.currency_code, fact.currency_code) as currency_code
from ledger_by_currency as ledger
full outer join fact_by_currency as fact
    on ledger.currency_code = fact.currency_code
where ledger.expected_entry_count is distinct from fact.actual_entry_count
   or ledger.expected_total_debits is distinct from fact.actual_total_debits
   or ledger.expected_total_credits is distinct from fact.actual_total_credits
   or ledger.expected_net_position is distinct from fact.actual_net_position
