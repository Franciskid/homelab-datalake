-- Typed LLM calls.
--
-- `spend_usd` is LiteLLM's own figure, not recomputed. Recomputing would mean carrying
-- a second copy of every model's price list, which would drift from the proxy's and
-- then quietly disagree with the bill. The ledger is the source of truth for cost.

with source as (

    select * from {{ source('raw', 'litellm_spend') }}

),

renamed as (

    select
        dt                                          as partition_date,
        request_id,
        nullif(session_id, '')                      as session_id,
        started_at,
        ended_at,
        first_token_at,

        nullif(call_type, '')                       as call_type,
        nullif(model, '')                           as model,
        nullif(model_group, '')                     as model_group,
        nullif(provider, '')                        as provider,
        nullif(mcp_tool, '')                        as mcp_tool,
        coalesce(nullif(status, ''), 'unknown')     as status,

        coalesce(spend_usd, 0.0)                    as spend_usd,
        coalesce(total_tokens, 0)                   as total_tokens,
        coalesce(prompt_tokens, 0)                  as prompt_tokens,
        coalesce(completion_tokens, 0)              as completion_tokens,
        duration_ms,

        -- LiteLLM writes '' for a miss and 'True'/'true' for a hit, so this is a
        -- three-state field flattened to a boolean deliberately: unknown is a miss for
        -- accounting purposes, because an uncached call is what got billed.
        (lower(coalesce(cache_hit, '')) in ('true', 't', '1'))  as was_cache_hit,

        nullif(team_id, '')                         as team_id,
        end_user                                    as end_user_hash,
        api_key_hash,
        client_hash,
        client_network,
        client_trust,
        geo_country_code,
        geo_asn,
        geo_as_org,
        model_id,
        api_base,
        cache_key,
        organization_id,
        agent_id,
        user_hash,
        request_tags,
        cached_tokens,
        prompt_audio_tokens,
        reasoning_tokens,
        completion_audio_tokens,
        accepted_prediction_tokens,
        rejected_prediction_tokens,
        input_cost_usd,
        output_cost_usd,
        tool_usage_cost_usd,
        original_cost_usd,
        discount_amount_usd,
        attempted_retries,
        max_retries,
        litellm_overhead_ms,
        applied_guardrails,
        api_key_alias,
        team_alias,
        model_map_key,
        error_information,
        metadata_json,
        requester_ip,
        messages_json,
        response_json,
        proxy_server_request_json

    from source

)

select
    *,
    -- Time to first token is the number a user actually feels; total duration is what
    -- the bill reflects. They are different questions, so both are kept.
    case when first_token_at is not null and started_at is not null
         then datediff('millisecond', started_at, first_token_at)
    end                                             as time_to_first_token_ms,

    case
        when call_type in ('list_mcp_tools', 'call_mcp_tool') then 'mcp'
        when call_type like '%embedding%'                     then 'embedding'
        when call_type in ('acompletion', 'completion', 'responses') then 'completion'
        else 'other'
    end                                             as workload,

    -- coalesce is load-bearing. `provider` is null on ~[count withheld] rows, and in SQL
    -- `null = 'ollama'` is null, not false -- so `null or false` yields null and the
    -- column comes back with three values instead of two. A boolean that is sometimes
    -- null silently drops rows from every `where is_self_hosted = false`.
    coalesce(provider = 'ollama', false)
      or coalesce(model like 'ollama%', false)      as is_self_hosted

from renamed
