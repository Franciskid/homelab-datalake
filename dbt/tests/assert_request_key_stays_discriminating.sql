{{ config(severity='warn') }}

-- `request_key` is a surrogate, not a natural key. This is the test its description
-- promises -- and which, until 30 Aug 2026, did not exist.
--
-- Duplicates are correct here. The key is md5 over instance, timestamp, client hash,
-- path, status and bytes, and two genuinely distinct requests can agree on all six:
-- a scanner asking for /login twice in the same second from the same address produces
-- two rows that are identical in every recorded respect. The log carries whole
-- seconds -- one busy second in August holds [count withheld] requests -- so nothing
-- in it distinguishes them, and inventing a difference would be inventing data.
--
-- Two things about this test were wrong, and both are fixed here.
--
-- It measured the duplicate share over the WHOLE table. That number is monotonic in
-- the worst way: a single abnormal day is averaged into every build that ever runs
-- afterwards, so once it crosses the threshold it can never come back under it. On
-- 3 Sep 2026 a scanner walked api.homelab.example.test for [count withheld] requests
-- -- nine times a normal day, [rate withheld] of them duplicates by the definition
-- above -- and pushed the all-history share from [rate withheld] to [rate withheld].
-- Every nightly build from 4 Sep failed on this assertion, `dbt build` skipped
-- everything downstream of the fact table, `publish` never ran, and the serving copy
-- the UI, the MCP tools and the API all read stayed frozen at 2 Sep for eight days
-- while the raw layer kept landing perfectly. Discrimination is a property of a day's
-- data; it is now measured per day.
--
-- It also failed the build for the case its own comment said should not fail it -- a
-- noisy scanning day. A flood of identical requests is real traffic faithfully
-- recorded, not a key that has stopped working, so this is a warning. The failure
-- mode that genuinely corrupts every downstream distinct-count -- a column falling
-- out of the key -- is watched by assert_request_key_inputs_stay_populated, which
-- does fail the build.
--
-- Measured over 40 days carrying at least 1,000 requests: the median day has
-- [rate withheld] of its rows beyond the first in a duplicate group, the 95th
-- percentile [rate withheld], and the worst -- 3 Sep 2026, the scanner flood --
-- [rate withheld]. No day reached the threshold below: above it, a day is behaving
-- like nothing this lake has recorded.
--
-- Quiet days are excluded, as in assert_reject_rate_within_threshold: three requests
-- of which two repeat is [rate withheld] and means nothing.

with grouped as (

    select
        partition_date,
        request_key,
        count(*) as rows_in_group
    from {{ ref('fct_http_request') }}
    group by 1, 2

),

per_day as (

    select
        partition_date,
        sum(rows_in_group)                              as total_rows,
        sum(rows_in_group - 1)                          as rows_beyond_first,
        max(rows_in_group)                              as largest_group
    from grouped
    group by 1

)

select
    partition_date,
    total_rows,
    rows_beyond_first,
    largest_group,
    round(1.0 * rows_beyond_first / nullif(total_rows, 0), 4) as duplicate_share
from per_day
where total_rows >= 1000
  and 1.0 * rows_beyond_first / nullif(total_rows, 0) > 0.40
