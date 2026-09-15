-- One row per agent run -- every turn of work the platform did, whether or not it
-- called the gateway. This is where "how many runs failed today" and "what is a run
-- costing end to end" live; fct_ai_gateway_call is one call inside one run, and a run
-- can hold several calls, or none at all if it never reached the gateway.

select
    r.run_id                                    as run_key,
    r.partition_date,
    r.started_at,
    date_trunc('hour', r.started_at)            as started_hour,
    r.completed_at,
    r.duration_ms,

    r.conversation_id,
    c.title                                     as conversation_title,
    c.agent_id,
    coalesce(ag.agent_name, c.agent_id)         as agent_name,

    r.initiated_by_user_id,
    r.status,
    r.requested_model,
    r.actual_route,
    (r.requested_model is distinct from r.actual_route)  as was_rerouted,
    r.failure_code,

    r.input_tokens,
    r.output_tokens,
    coalesce(r.input_tokens, 0) + coalesce(r.output_tokens, 0)  as total_tokens,
    r.reasoning_tokens,
    r.cost_eur,

    r.awaiting_input_deadline,
    (r.status = 'awaiting_input')               as is_awaiting_input,
    r.report_definition_id,
    (r.report_definition_id is not null)        as is_report_run,

    r.metadata

from {{ ref('stg_ai_platform_runs') }} r
left join {{ ref('stg_ai_platform_conversations') }} c on r.conversation_id = c.conversation_id
left join {{ ref('stg_ai_platform_agents') }} ag on c.agent_id = ag.id
