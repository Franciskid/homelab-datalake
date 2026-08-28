-- One row per LLM call. The grain the MCP layer answers questions at.

select
    request_id                                  as call_key,
    partition_date,
    started_at,
    date_trunc('hour', started_at)              as started_hour,

    model,
    model_group,
    provider,
    workload,
    call_type,
    mcp_tool,
    status,
    is_self_hosted,
    was_cache_hit,

    spend_usd,
    total_tokens,
    prompt_tokens,
    completion_tokens,
    -- Guarding the divisor rather than the numerator: a zero-token call is real
    -- (a failure, or a cache hit) and must not become a null row.
    case when total_tokens > 0 then spend_usd / total_tokens end as usd_per_token,

    duration_ms,
    time_to_first_token_ms,

    team_id,
    end_user_hash,
    api_key_hash,
    client_hash,
    client_trust,
    geo_country_code,
    geo_as_org,
    session_id

from {{ ref('stg_litellm_spend') }}
