-- The failure mode assert_request_key_stays_discriminating cannot see, watched directly.
-- That test catches a key merging rows that differ; this one catches inputs that have
-- stopped differing, which makes rows identical and so passes it.
--
-- `request_key` is md5 over every recorded field (thirteen since 19 Sep 2026; six
-- before). The core ones are checked below. If one of them stops carrying information --
-- client_hash null across the board after a hashing change, bytes_sent always zero
-- after NPM changes its log format, a path that never parses -- the key keeps being
-- produced and keeps looking like a key, while quietly collapsing rows that are not
-- the same request. Every distinct-count downstream shrinks, and nothing errors:
-- the dashboards simply report fewer visitors than there were.
--
-- Watching the duplicate share was an indirect way of noticing that, and it could not
-- tell the difference between a column falling out of the key and a scanner having a
-- busy night. These conditions can: each one is a column that has stopped saying
-- anything, which no amount of unusual traffic produces.
--
-- This one fails the build rather than warning. A key that no longer discriminates
-- makes every number built on top of it wrong, and publishing that is worse than
-- publishing nothing -- the same argument the privacy tests are held to.
--
-- The thresholds are far from the observed values on purpose, so ordinary variation
-- never reaches them. Measured across 40 days of real traffic:
--
--   client_hash      null on 0.00% of rows, every day. 9,281 distinct hashes overall,
--                    209 to 2,331 on any given day. Fails above 50% null, or when a
--                    whole day resolves to a single client.
--   path             null on at most 0.44% of rows. Fails above 50%.
--   status           never null. Fails above 50%.
--   bytes_sent       zero on 10.1% of rows overall, at most 17.5% on a day -- 304s,
--                    redirects and rejected requests genuinely send no body. Fails
--                    above 95%, which is a log format that has stopped reporting it.
--   requested_at     thousands of distinct seconds a day. Fails at fewer than two,
--                    which is a timestamp that has stopped being parsed.
--
-- Quiet days are excluded for the reason they are excluded everywhere else here: a
-- handful of requests cannot support a share.

with per_day as (

    select
        partition_date,
        count(*)                                                        as total_rows,
        1.0 * count(*) filter (where client_hash is null) / count(*)    as client_hash_null_share,
        count(distinct client_hash)                                     as distinct_client_hashes,
        1.0 * count(*) filter (where path is null) / count(*)           as path_null_share,
        1.0 * count(*) filter (where status is null) / count(*)         as status_null_share,
        1.0 * count(*) filter (where coalesce(bytes_sent, 0) = 0) / count(*)
                                                                        as bytes_zero_share,
        count(distinct requested_at)                                    as distinct_timestamps
    from {{ ref('fct_http_request') }}
    group by 1

)

select
    partition_date,
    total_rows,
    round(client_hash_null_share, 4) as client_hash_null_share,
    distinct_client_hashes,
    round(path_null_share, 4)        as path_null_share,
    round(status_null_share, 4)      as status_null_share,
    round(bytes_zero_share, 4)       as bytes_zero_share,
    distinct_timestamps
from per_day
where total_rows >= 1000
  and (
        client_hash_null_share > 0.50
     or distinct_client_hashes < 2
     or path_null_share        > 0.50
     or status_null_share      > 0.50
     or bytes_zero_share       > 0.95
     or distinct_timestamps    < 2
  )
