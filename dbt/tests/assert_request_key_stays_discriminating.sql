-- `request_key` is a surrogate, not a natural key. This is the test its description
-- promises -- and which, until 30 Aug 2026, did not exist.
--
-- Duplicates are correct here. The key is md5 over instance, timestamp, client hash,
-- path, status and bytes, and two genuinely distinct requests can agree on all six:
-- a scanner asking for /login twice in the same second from the same address produces
-- two rows that are identical in every recorded respect. Nothing in an access log
-- distinguishes them, so inventing a difference would be inventing data.
--
-- What must not happen is the key quietly losing its power to discriminate. If a
-- column that feeds it stopped being populated -- client_hash going null across the
-- board, bytes_sent always zero after a log-format change -- the duplicate share would
-- climb and every downstream distinct-count would silently shrink. That is what this
-- watches.
--
-- Measured across [count withheld] rows and 34 days: [rate withheld] of rows sit beyond the first in a
-- duplicate group, and the largest group is 72 -- a scanner walking
-- api.homelab.example.test. The threshold is [rate withheld], roughly three times the observed rate:
-- wide enough that a noisy scanning day does not fail the build, narrow enough that a
-- column falling out of the key does.

with grouped as (

    select
        request_key,
        count(*) as rows_in_group
    from {{ ref('fct_http_request') }}
    group by 1

),

summary as (

    select
        sum(rows_in_group)                              as total_rows,
        sum(rows_in_group - 1)                          as rows_beyond_first,
        max(rows_in_group)                              as largest_group
    from grouped

)

select
    total_rows,
    rows_beyond_first,
    largest_group,
    round(1.0 * rows_beyond_first / nullif(total_rows, 0), 4) as duplicate_share
from summary
where 1.0 * rows_beyond_first / nullif(total_rows, 0) > 0.15
