-- No title may be valid in two versions at once.
--
-- The failure this catches is subtle: an overlap makes an "as of" query return two
-- rows for one title, so every count silently doubles for the affected titles rather
-- than erroring. Exactly one open version per title, and no closed version may extend
-- past the next one's start.

with versions as (

    select
        item_id,
        version,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by item_id order by version) as next_valid_from
    from {{ ref('dim_title_scd2') }}

)

select item_id, version, valid_from, valid_to, next_valid_from
from versions
where
    -- A closed version overlapping the one that follows it.
    (valid_to is not null and next_valid_from is not null and valid_to > next_valid_from)
    -- An open version that is not the latest.
    or (valid_to is null and next_valid_from is not null)
    -- Backwards interval.
    or (valid_to is not null and valid_to < valid_from)
