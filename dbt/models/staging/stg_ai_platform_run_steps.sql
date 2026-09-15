-- Every step of every run, typed. `payload` passes through as JSON text unparsed --
-- fct_agent_tool_call is what picks fields out of it, because the shape depends on
-- `type` and only a tool_call step has tool detail worth a column.

with source as (

    select * from {{ source('raw', 'platform_run_steps') }}

)

select
    dt                                          as partition_date,
    id                                           as run_step_id,
    run_id,
    sequence,
    nullif(type, '')                            as step_type,
    nullif(title, '')                           as step_title,
    nullif(description, '')                     as step_description,
    payload,
    occurred_at

from source
