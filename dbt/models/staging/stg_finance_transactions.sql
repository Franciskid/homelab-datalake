-- The transactions the banking tracker has now, each at its newest version.
--
-- The raw table is a ledger (parsing/ledger.py): every version of every row. The current
-- version of a key is its newest one, and a key the newest read did not see is gone from the
-- tracker -- a duplicate the import folded away -- so it is left out here and kept in raw.

with ledger as (

    select * from {{ source('raw', 'finance_transactions') }}

),

newest_read as (

    select max(_last_seen_at) as read_at from ledger

),

current_versions as (

    select *
    from ledger
    qualify row_number() over (
        partition by transaction_id order by _last_seen_at desc, _first_seen_at desc
    ) = 1

)

select
    transaction_id,
    cast(booked_on as date)              as booked_on,
    account_id,
    account_name,
    merchant,
    cast(amount as double)               as amount,
    currency,
    cast(amount_home as double)          as amount_home,
    direction,
    category,
    category_label,
    category_group,
    nullif(transfer_category, '')        as transfer_category,
    type                                 as transaction_type,
    type_source,
    nullif(transfer_kind, '')            as transfer_kind,
    nullif(recurring_series_id, '')      as recurring_series_id,
    needs_review = 'True'                as needs_review,
    tags,
    nullif(notes, '')                    as notes,
    _first_seen_at                       as version_first_seen_at

from current_versions
where _last_seen_at = (select read_at from newest_read)
