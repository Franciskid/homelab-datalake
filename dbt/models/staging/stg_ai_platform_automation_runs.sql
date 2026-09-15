-- One row per run of an automation, typed.

with source as (

    select * from {{ source('raw', 'platform_automation_runs') }}

)

select
    dt                                          as partition_date,
    id                                           as automation_run_id,
    automation_id,
    nullif(dagster_run_id, '')                  as dagster_run_id,
    coalesce(nullif(status, ''), 'unknown')     as status,
    started_at,
    finished_at,
    case when finished_at is not null and started_at is not null
         then datediff('millisecond', started_at, finished_at)
    end                                          as duration_ms,
    created_at

from source
