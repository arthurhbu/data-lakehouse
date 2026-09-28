{{ config(materialized='table') }}

-- Grão: uma linha por parceiro e moeda do lançamento.
-- Como não há saldo inicial, net_position representa apenas a posição líquida
-- dos movimentos observados: créditos recebidos menos débitos enviados.

with ledger_with_partner as (
    select
        accounts.partner_id,
        ledger.currency as currency_code,
        ledger.entry_type,
        ledger.amount
    from {{ ref('stg_ledger_entries') }} as ledger
    inner join {{ ref('dim_accounts') }} as accounts
        on ledger.account_id = accounts.account_id
)

select
    partner_id,
    currency_code,
    count(*) as entry_count,
    sum(case when entry_type = 'debit' then amount else 0 end) as total_debits,
    sum(case when entry_type = 'credit' then -amount else 0 end) as total_credits,
    -sum(amount) as net_position
from ledger_with_partner
group by partner_id, currency_code
