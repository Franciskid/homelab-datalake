-- One row per tool call an agent run made. run_steps carries every kind of step --
-- reasoning, approval requests, tool calls -- and this mart is the tool_call slice of
-- it, which is the one a spend or an activity question actually wants: "how many
-- times did an agent call export_workbook today" has no answer in litellm_spend,
-- because a tool call is not a gateway call.
--
-- **The field extraction below is best-effort, and says so rather than pretending
-- otherwise.** `payload` is the application's own internal shape for a step, not a
-- documented contract this pipeline was given, and its exact keys were not verified
-- against a live sample when this model was written. `json_extract_string` returns
-- null for a path that is not there rather than raising, so an unrecognised shape
-- costs a run of null columns, not a broken build -- and `tool_name` falls back to
-- the step's own `title`, which is populated regardless of payload shape.

select
    s.run_step_id                               as tool_call_key,
    s.partition_date,
    s.occurred_at,
    date_trunc('hour', s.occurred_at)           as occurred_hour,

    s.run_id,
    s.sequence,

    coalesce(
        json_extract_string(s.payload, '$.tool_name'),
        json_extract_string(s.payload, '$.tool'),
        s.step_title
    )                                            as tool_name,
    coalesce(
        json_extract_string(s.payload, '$.server_alias'),
        json_extract_string(s.payload, '$.server'),
        json_extract_string(s.payload, '$.mcp_server')
    )                                            as server_alias,
    json_extract(s.payload, '$.arguments')      as arguments_json,
    json_extract(s.payload, '$.result')         as result_json,
    coalesce(
        json_extract_string(s.payload, '$.status'),
        -- run_steps carries no status of its own; a step that has an occurred_at and
        -- no explicit failure marker in its payload is read as having completed.
        case when json_extract_string(s.payload, '$.error') is not null
             then 'failed' else 'completed' end
    )                                            as call_status,
    json_extract_string(s.payload, '$.error')   as error_message,
    try_cast(json_extract(s.payload, '$.duration_ms') as bigint)  as duration_ms,

    s.step_title,
    s.step_description,
    s.payload

from {{ ref('stg_ai_platform_run_steps') }} s
where s.step_type = 'tool_call'
