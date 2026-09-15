-- Every agent run, typed.

with source as (

    select * from {{ source('raw', 'platform_runs') }}

)

select
    dt                                          as partition_date,
    id                                           as run_id,
    conversation_id,
    initiated_by_user_id,
    coalesce(nullif(status, ''), 'unknown')     as status,
    nullif(requested_model, '')                 as requested_model,
    nullif(actual_route, '')                    as actual_route,
    input_tokens,
    output_tokens,
    reasoning_tokens,
    cost_eur,
    started_at,
    completed_at,
    -- What a person actually feels waiting on a run, and the figure agg_llm_spend_daily's
    -- own median-latency measure has no equivalent for at the run grain.
    case when completed_at is not null and started_at is not null
         then datediff('millisecond', started_at, completed_at)
    end                                          as duration_ms,
    nullif(failure_code, '')                    as failure_code,
    metadata,
    awaiting_input_deadline,
    report_definition_id

from source
