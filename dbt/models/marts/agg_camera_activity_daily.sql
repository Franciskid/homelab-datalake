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
    count(*) filter (where was_corrected)                   as corrected,
    count(*) filter (where has_clip)                        as with_clip,

    -- The subject totals. This is the row the alerts graph is drawn from, and until
    -- `counts` was carried through ingest there was nothing here to draw it with.
    -- `sum` skips nulls, so a day where nothing was counted reads as null rather than
    -- as a measured zero -- and `counted_events` beside it says which it was.
    sum(count_person)                                       as people,
    sum(count_vehicle)                                      as vehicles,
    sum(count_animal)                                       as animals,
    sum(count_other)                                        as other_subjects,
    sum(count_total)                                        as subjects,
    count(*) filter (where count_total is not null)         as counted_events,

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
