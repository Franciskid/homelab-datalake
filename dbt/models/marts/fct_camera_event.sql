-- One row per camera event, latest version. The grain analysis works at.

select
    event_id,
    partition_date,
    camera_id,
    zone_id,
    zone_name,
    rule_type,
    lifecycle,
    is_confirmed,
    is_awaiting_analysis,

    started_at,
    completed_at,
    duration_seconds,
    date_trunc('hour', started_at)                          as started_hour,
    -- Hour-of-day and weekday are the analysis on event data: when does the garden get
    -- busy, does the weekend differ. Derived in UTC, pinned by the session setting.
    extract('hour' from started_at)                         as started_hour_of_day,
    dayname(started_at)                                     as started_weekday,
    (dayofweek(started_at) in (0, 6))                       as is_weekend,

    direction,
    zone_state,
    previous_zone_state,
    (zone_state is distinct from previous_zone_state)       as state_changed,

    confidence,
    is_unusual,
    should_notify,
    dismissal_reason,
    duplicate_of,
    (duplicate_of is not null)                              as is_duplicate,
    summary,
    clip_id,
    extracted_at

from {{ ref('int_camera_events') }}
