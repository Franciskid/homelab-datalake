-- One row per transaction the tracker has now. Amounts are signed (negative is money out);
-- amount_home is in the owner's home currency, converted by the tracker itself.
--
-- is_spending / is_income leave transfers out: money moved between the owner's own accounts,
-- or to and from savings, is neither spent nor earned.

select
    t.transaction_id                                      as transaction_key,
    t.booked_on,
    cast(date_trunc('month', t.booked_on) as date)       as booked_month,
    t.account_id,
    coalesce(a.account_name, t.account_name)              as account_name,
    a.bank,
    t.merchant,
    t.amount,
    t.currency,
    t.amount_home,
    t.direction,
    t.category,
    t.category_label,
    t.category_group,
    t.transfer_category,
    t.transaction_type,
    t.type_source,
    t.transfer_kind,
    t.recurring_series_id,
    t.needs_review,
    t.tags,
    t.notes,
    t.description,
    t.transaction_status,
    t.posted_at,
    t.source,
    t.source_transaction_id,
    t.metadata,
    t.tag_slugs,
    t.property_links,
    (t.transaction_type <> 'transfer' and t.amount_home < 0) as is_spending,
    (t.transaction_type <> 'transfer' and t.amount_home > 0) as is_income

from {{ ref('stg_finance_transactions') }} as t
left join {{ ref('stg_finance_accounts') }} as a using (account_id)
