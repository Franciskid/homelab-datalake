-- Exactly one current version per title in the library, and none for a title that has
-- left it. Two would double every "what is in the library now" count; zero for a title
-- still on the shelf would make it vanish from that count entirely.
--
-- The second half of that sentence is what this test was missing until 19 Sep 2026.
-- It read `having count(*) filter (where is_current) <> 1` over every item_id the
-- dimension had ever held, which asserts that no title ever leaves the library. On
-- 17 Sep 2026 one did -- 'La Cité des Anges', last seen on the 16th -- and the nightly
-- build failed on a single row for three days. `dbt build` stops at a failed test, so
-- nothing downstream of the dimension was rebuilt and the serving copy froze while the
-- lake kept landing correctly: the same shape of outage as the request_key threshold
-- in September, from the same cause, a test encoding an assumption the world was free
-- to break.
--
-- A deletion is now a fact the model states (`retired_on`) rather than an absence this
-- test has to interpret, which is what lets both rules be checked at once:
--
--   * a title still in the latest snapshot has exactly one current version;
--   * a retired title has none, and is retired on exactly one of its versions -- the
--     last. More than one would mean the model had reopened a closed title.
--
-- What this deliberately does NOT do is accept a mass disappearance as retirement.
-- 2,261 titles "retired" on 19 Sep 2026 when Jellyfin 12.1.0 began collapsing
-- collections; that is caught where it happens, by the shrink guard in
-- `jellyfin_library_raw`, which now refuses to write such a snapshot at all. A test
-- here could only say the marts faithfully recorded whatever the lake was given.

with per_title as (

    select
        item_id,
        count(*)                                  as versions,
        count(*) filter (where is_current)        as current_versions,
        count(*) filter (where retired_on is not null) as retirement_rows,
        max(retired_on)                           as retired_on
    from {{ ref('dim_title_scd2') }}
    group by item_id

)

select item_id, versions, current_versions, retirement_rows, retired_on
from per_title
where
    -- In the library now: exactly one row describes it.
    (retired_on is null and current_versions <> 1)
    -- Gone: nothing describes it as current.
    or (retired_on is not null and current_versions <> 0)
    -- Retired twice is a contradiction, whatever the counts say.
    or retirement_rows > 1
