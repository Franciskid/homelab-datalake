-- One row per call through the api.ai gateway. The application name and key label are
-- joined in from their own snapshotted registries -- see stg_ai_platform_applications
-- and stg_ai_platform_api_keys for why those are deduped to "as of today" rather than
-- carrying their own snapshot history into this join.
--
-- Zero tokens is a real, common state here, not a data quality problem: a streamed
-- call whose caller never asked the gateway for usage figures reports nothing back,
-- and a failed call before the model answered reports nothing either. Neither should
-- read as "this call was free" the way summing tokens across a period naively would.

select
    e.usage_event_id                            as call_key,
    e.partition_date,
    e.created_at,
    date_trunc('hour', e.created_at)            as created_hour,

    e.application_id,
    coalesce(a.application_name, e.application_id)  as application_name,
    e.api_key_id,
    coalesce(k.key_label, e.api_key_id)         as key_label,

    e.call_source,
    e.client_name,
    e.client_version,
    e.actor_label,
    e.actor_external_id,
    e.requested_model,
    e.actual_model,
    -- Not the same value on every row: the platform can route a requested alias to a
    -- different served model, the same distinction litellm_spend.model_group makes.
    (e.requested_model is distinct from e.actual_model)  as was_rerouted,
    e.status,
    e.failure_code,

    e.input_tokens,
    e.output_tokens,
    e.input_tokens + e.output_tokens            as total_tokens,
    e.cost_eur,
    e.latency_ms,

    e.conversation_id,
    e.prompt,
    e.response,
    e.results,
    e.metadata,

    e.frame_count,
    e.frame_bytes,
    e.frame_keys,
    (e.frame_count > 0)                         as has_frames

from {{ ref('stg_ai_platform_usage_events') }} e
left join {{ ref('stg_ai_platform_applications') }} a on e.application_id = a.id
left join {{ ref('stg_ai_platform_api_keys') }} k on e.api_key_id = k.id
