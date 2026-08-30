-- The daily spend aggregate must equal the fact table it is built from.
--
-- This exists because of a rounding fault that was live for the whole of
-- agg_llm_spend_daily's history and that no test caught. DuckDB's bare NUMERIC is
-- DECIMAL(18,3), so `round(sum(spend_usd)::numeric, 6)` rounded to three decimals and
-- the 6 did nothing. The aggregate reported [spend withheld] against the fact table's [spend withheld],
-- and 26 of [count withheld] rows showed a usd_per_call of [spend withheld] against real spend.
--
-- A row-count test would not have found it: the counts always agreed. Only comparing
-- the *money* does.
--
-- The threshold is 1e-6, which is the precision the aggregate claims to keep. It is not
-- zero, because the aggregate is stored rounded on purpose -- a stored figure that
-- carries more digits than it can defend is its own kind of lie.

with fact as (

    select
        partition_date,
        model,
        provider,
        workload,
        count(*)        as calls,
        sum(spend_usd)  as spend_usd
    from {{ ref('fct_llm_call') }}
    group by 1, 2, 3, 4

),

agg as (

    select
        partition_date,
        model,
        provider,
        workload,
        calls,
        spend_usd
    from {{ ref('agg_llm_spend_daily') }}

)

select
    coalesce(fact.partition_date, agg.partition_date) as partition_date,
    coalesce(fact.model, agg.model)                   as model,
    fact.calls                                        as fact_calls,
    agg.calls                                         as agg_calls,
    fact.spend_usd                                    as fact_spend_usd,
    agg.spend_usd                                     as agg_spend_usd
from fact
full outer join agg
    on  fact.partition_date  is not distinct from agg.partition_date
    and fact.model           is not distinct from agg.model
    and fact.provider        is not distinct from agg.provider
    and fact.workload        is not distinct from agg.workload
where fact.calls is distinct from agg.calls
   or abs(coalesce(fact.spend_usd, 0) - coalesce(agg.spend_usd, 0)) > 0.000001
