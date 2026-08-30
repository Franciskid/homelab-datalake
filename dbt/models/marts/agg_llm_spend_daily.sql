-- Daily spend by model and workload. The table the MCP layer reads for
-- "what did I spend on Claude in August?"

-- A note on the casts below.
--
-- DuckDB's bare NUMERIC is DECIMAL(18,3). So `round(x::numeric, 6)` rounds to three
-- decimals and the 6 does nothing -- the cast has already thrown the digits away. That
-- was live for the whole of this table's history: 26 of [count withheld] rows reported a
-- usd_per_call of [spend withheld] against real spend, because a mistral-small call costs about
-- [spend withheld]. The daily total was low by [spend withheld] over [spend withheld].
--
-- So every cast that feeds a round() states its own precision. DECIMAL(18,9) leaves
-- nine digits after the point, which is four more than the cheapest model needs, and
-- still nine digits of headroom before the 18 total is reached.

select
    partition_date,
    model,
    provider,
    workload,

    count(*)                                                as calls,
    count(*) filter (where status = 'failure')              as failures,
    count(*) filter (where was_cache_hit)                   as cache_hits,
    count(distinct session_id)                              as sessions,

    round(sum(spend_usd)::decimal(18, 9), 6)                as spend_usd,
    sum(total_tokens)                                       as total_tokens,
    sum(prompt_tokens)                                      as prompt_tokens,
    sum(completion_tokens)                                  as completion_tokens,

    -- Median, not mean: a single 90-second agent run drags an average latency far
    -- enough to hide what a normal call actually costs in time.
    round(median(duration_ms)::decimal(18, 3), 1)           as median_duration_ms,
    round(median(time_to_first_token_ms)::decimal(18, 3), 1) as median_ttft_ms,
    round((sum(spend_usd) / nullif(count(*), 0))::decimal(18, 9), 6) as usd_per_call

from {{ ref('fct_llm_call') }}
group by 1, 2, 3, 4
