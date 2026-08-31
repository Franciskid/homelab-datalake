-- One row per span where a zone could not be read.
--
-- The table that makes a gate report honest. A reading of "closed all afternoon" means
-- one thing when the camera could see the gate throughout and quite another when it
-- could not see it at all -- and until this existed the warehouse could not tell them
-- apart, so it drew the second as the first.

with spans as (select * from {{ ref('stg_camera_observability') }}),

zones as (select * from {{ ref('dim_camera_zone') }})

select
    spans.span_id,
    spans.partition_date,
    spans.camera_id,
    spans.zone_id,
    -- Joined on the zone, not the rule: a span is about the zone being unreadable,
    -- which is true of every rule on it at once. `any_value` because a zone with two
    -- rules is two rows in the dimension and the zone-level columns are identical
    -- across them -- a plain join would double every span.
    any_value(zones.zone_name)                          as zone_name,
    any_value(zones.is_gate_rule)                       as is_on_a_gate_zone,

    spans.reason,
    spans.detail,
    spans.is_lease_expiry,
    spans.is_vision_loss,
    spans.from_at,
    spans.to_at,
    spans.is_ongoing,
    spans.duration_seconds,
    round(spans.duration_seconds / 60.0, 2)             as duration_minutes,
    date_trunc('hour', spans.from_at)                   as from_hour,
    extract('hour' from spans.from_at)                  as from_hour_of_day,
    dayname(spans.from_at)                              as from_weekday,

    spans.unmapped_json,
    spans.extracted_at

from spans
left join zones on zones.zone_id = spans.zone_id
group by
    spans.span_id, spans.partition_date, spans.camera_id, spans.zone_id,
    spans.reason, spans.detail, spans.is_lease_expiry, spans.is_vision_loss,
    spans.from_at, spans.to_at, spans.is_ongoing, spans.duration_seconds,
    spans.unmapped_json, spans.extracted_at
