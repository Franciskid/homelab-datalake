-- Daily activity by camera, zone and rule.

select
    partition_date,
    camera_id,
    zone_name,
    rule_type,

    count(*)                                                as events,
    count(*) filter (where is_confirmed)                    as confirmed,
    count(*) filter (where lifecycle = 'dismissed')         as dismissed,
    count(*) filter (where is_awaiting_analysis)            as awaiting_analysis,
    count(*) filter (where should_notify)                   as notified,
    count(*) filter (where is_unusual)                      as unusual,
    count(*) filter (where is_duplicate)                    as duplicates,

    -- Confidence is null when the model would not commit, and avg() skips nulls --
    -- so the count of scored events travels beside the average. An average over three
    -- events reads identically to one over three hundred otherwise.
    count(*) filter (where confidence is not null)          as scored_events,
    -- decimal(18,9), not numeric: DuckDB's bare NUMERIC is DECIMAL(18,3), which
    -- would round a confidence of 0.8125 to 0.813 and make the 4 above a lie.
    round(avg(confidence)::decimal(18, 9), 4)               as avg_confidence,
    round(median(duration_seconds)::decimal(18, 3), 1)      as median_duration_seconds

from {{ ref('fct_camera_event') }}
group by 1, 2, 3, 4
