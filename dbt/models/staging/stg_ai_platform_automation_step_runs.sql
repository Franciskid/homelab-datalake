-- One row per step of one automation run, typed.

with source as (

    select * from {{ source('raw', 'platform_automation_step_runs') }}

)

select
    dt                                          as partition_date,
    id                                           as automation_step_run_id,
    automation_run_id,
    step_id,
    nullif(dagster_run_id, '')                  as dagster_run_id,
    coalesce(nullif(status, ''), 'unknown')     as status,
    outputs,
    nullif(error, '')                           as error,
    started_at,
    finished_at,
    step_position,
    nullif(step_type, '')                       as step_type,
    nullif(step_label, '')                      as step_label,
    step_config

from source
