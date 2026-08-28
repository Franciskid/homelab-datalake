-- Every row must sit in the partition it was written to.
--
-- This is the idempotency property expressed as data rather than as a claim. The
-- partition is defined by the timestamp inside each line, so a row whose timestamp
-- falls outside its own partition_date means the day filter and the file contents
-- disagree. That would make a re-run produce different output, and would break
-- backfill silently -- the two properties the whole design is built around.
--
-- It also guards the UTC session-timezone pin in profiles.yml. Casting a
-- TIMESTAMPTZ to a date renders it in the session timezone, so if that pin is ever
-- removed this test starts failing on any machine that is not on UTC. That is the
-- correct outcome: the pipeline really would be assigning rows to the wrong day.

select
    partition_date,
    npm_instance,
    min(requested_at) as earliest,
    max(requested_at) as latest,
    count(*)          as offending_rows
from {{ ref('fct_http_request') }}
where cast(requested_at as date) <> partition_date
group by 1, 2
