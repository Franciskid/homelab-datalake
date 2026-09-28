-- Subscriptions and bills the tracker detects now, newest version of each. An ended series
-- (no charge where its cadence said one was due) is still here, with status 'ended'.

with ledger as (

    select * from {{ source('raw', 'finance_recurring_series') }}

),

newest_read as (

    select max(_last_seen_at) as read_at from ledger

)

select
    series_id,
    series_name,
    type                                         as series_type,
    status,
    category,
    nullif(account_id, '')                       as account_id,
    nullif(account_name, '')                     as account_name,
    frequency,
    currency,
    cast(predicted_next_amount as double)        as predicted_next_amount,
    cast(monthly_projected_amount as double)     as monthly_projected_amount,
    cast(monthly_projected_home as double)       as monthly_projected_home,
    cast(nullif(next_due_date, '') as date)      as next_due_on,
    cast(nullif(first_seen, '') as date)         as first_charged_on,
    cast(nullif(last_seen, '') as date)          as last_charged_on,
    cast(charge_count as integer)                as charge_count

from ledger
where _last_seen_at = (select read_at from newest_read)
qualify row_number() over (partition by series_id order by _first_seen_at desc) = 1
