-- Each account's balance per day: the last reading taken that day.

select
    dt                    as balance_date,
    account_id,
    account_name,
    bank,
    account_type,
    role,
    currency,
    balance,
    home_currency,
    balance_home

from {{ source('raw', 'finance_account_balances') }}
qualify row_number() over (partition by dt, account_id order by captured_at desc) = 1
