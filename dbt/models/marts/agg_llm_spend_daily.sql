-- Daily spend by model and workload. The table the MCP layer reads for
-- "what did I spend on Claude in August?"

select
    partition_date,
    model,
    provider,
    workload,

    count(*)                                                as calls,
    count(*) filter (where status = 'failure')              as failures,
    count(*) filter (where was_cache_hit)                   as cache_hits,
    count(distinct session_id)                              as sessions,

    round(sum(spend_usd)::numeric, 6)                       as spend_usd,
    sum(total_tokens)                                       as total_tokens,
    sum(prompt_tokens)                                      as prompt_tokens,
    sum(completion_tokens)                                  as completion_tokens,

    -- Median, not mean: a single 90-second agent run drags an average latency far
    -- enough to hide what a normal call actually costs in time.
    round(median(duration_ms)::numeric, 1)                  as median_duration_ms,
    round(median(time_to_first_token_ms)::numeric, 1)       as median_ttft_ms,
    round((sum(spend_usd) / nullif(count(*), 0))::numeric, 6) as usd_per_call

from {{ ref('fct_llm_call') }}
group by 1, 2, 3, 4
