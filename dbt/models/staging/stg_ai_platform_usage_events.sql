-- Every call through the api.ai gateway, typed. `prompt` and `response` pass through
-- unchanged: the owner's 2026-09-14 decision is to land them in full, and there is no
-- cleaning to do to free text. `metadata` already had its base64 frames stripped out
-- at ingestion -- see ai_platform.py -- so what remains here is small.

with source as (

    select * from {{ source('raw', 'platform_usage_events') }}

)

select
    dt                                          as partition_date,
    id                                           as usage_event_id,
    application_id,
    api_key_id,
    nullif(source, '')                          as call_source,
    nullif(client_name, '')                     as client_name,
    nullif(client_version, '')                  as client_version,
    nullif(actor_label, '')                     as actor_label,
    nullif(actor_external_id, '')               as actor_external_id,
    nullif(requested_model, '')                 as requested_model,
    nullif(actual_model, '')                    as actual_model,
    prompt,
    response,
    results,
    coalesce(input_tokens, 0)                   as input_tokens,
    coalesce(output_tokens, 0)                  as output_tokens,
    coalesce(cost_eur, 0.0)                     as cost_eur,
    latency_ms,
    coalesce(nullif(status, ''), 'unknown')     as status,
    nullif(failure_code, '')                    as failure_code,
    metadata,
    created_at,
    nullif(conversation_id, '')                 as conversation_id,
    frame_keys,
    coalesce(frame_count, 0)                    as frame_count,
    coalesce(frame_bytes, 0)                    as frame_bytes

from source
