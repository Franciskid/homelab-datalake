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
        geo_as_org

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

    (provider = 'ollama' or model like 'ollama%')   as is_self_hosted

from renamed
