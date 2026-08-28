-- Fails the build when a day's unparseable share crosses 1%.
--
-- The measured rate across 10,818 real lines from both hosts was 0.00%, so this
-- threshold is not a guess about acceptable loss -- it is an alarm for NPM changing
-- its log format. A pipeline that keeps running while it has stopped understanding
-- its input is worse than one that stops, because the dashboard stays green.
--
-- Days with almost no traffic are excluded: one reject out of three lines is 33% and
-- means nothing.

with per_day as (

    select
        partition_date,
        npm_instance,
        count(*) as parsed_rows
    from {{ ref('fct_http_request') }}
    group by 1, 2

),

rejects as (

    select
        dt   as partition_date,
        host as npm_instance,
        count(*) as rejected_rows
    from {{ source('raw', 'npm_access_rejects') }}
    group by 1, 2

),

joined as (

    select
        p.partition_date,
        p.npm_instance,
        p.parsed_rows,
        coalesce(r.rejected_rows, 0) as rejected_rows,
        p.parsed_rows + coalesce(r.rejected_rows, 0) as total_lines
    from per_day p
    left join rejects r
        on p.partition_date = r.partition_date
       and p.npm_instance  = r.npm_instance

)

select
    partition_date,
    npm_instance,
    parsed_rows,
    rejected_rows,
    round(1.0 * rejected_rows / nullif(total_lines, 0), 4) as reject_rate
from joined
where total_lines >= 100
  and 1.0 * rejected_rows / nullif(total_lines, 0) > 0.01
