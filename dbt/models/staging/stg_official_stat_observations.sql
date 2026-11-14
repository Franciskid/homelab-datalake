-- The current value of every official figure: per key, the version read most recently.
-- Unlike the finance ledgers, a key is not dropped when a later read did not return it: a
-- publisher that was unreachable for a night leaves its figures as they were.

with ledger as (

    select * from {{ source('raw', 'official_stat_observations') }}

),

versions as (

    select source, dataset, series_key, geo, period, count(*) as versions
    from ledger
    group by all

)

select
    l.source || '|' || l.dataset || '|' || l.series_key || '|' || l.geo || '|' || l.period as observation_key,
    l.source,
    l.dataset,
    l.series_key,
    l.geo,
    l.period,
    cast(l.value as double)          as value,
    nullif(l.status, '')             as status,
    nullif(l.unit, '')               as unit,
    l.dims_json,
    l.dataset_label,
    l.source_updated,
    l.source_url,
    l._first_seen_at                 as first_seen_at,
    l._last_seen_at                  as last_seen_at,
    v.versions

from ledger as l
join versions as v using (source, dataset, series_key, geo, period)
qualify row_number() over (
    partition by l.source, l.dataset, l.series_key, l.geo, l.period
    order by l._last_seen_at desc, l._first_seen_at desc
) = 1
