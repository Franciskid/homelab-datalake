select
    observation_key,
    source,
    dataset,
    series_key,
    geo,
    period,
    try_cast(left(period, 4) as integer) as period_year,
    value,
    status,
    unit,
    dims_json,
    dataset_label,
    source_updated,
    source_url,
    first_seen_at,
    last_seen_at,
    versions - 1 as revisions

from {{ ref('stg_official_stat_observations') }}
