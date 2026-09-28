-- Accounts as the tracker has them now, newest version of each. See stg_finance_transactions
-- for how the ledger's current rows are picked.

with ledger as (

    select * from {{ source('raw', 'finance_accounts') }}

),

newest_read as (

    select max(_last_seen_at) as read_at from ledger

)

select
    account_id,
    account_name,
    bank,
    account_type,
    nullif(role, '')                     as role,
    currency,
    cast(balance as double)              as balance,
    cast(balance_home as double)         as balance_home,
    source,
    nullif(account_group, '')            as account_group,
    cast(nullif(opened_at, '') as date)  as opened_on

from ledger
where _last_seen_at = (select read_at from newest_read)
qualify row_number() over (partition by account_id order by _first_seen_at desc) = 1
