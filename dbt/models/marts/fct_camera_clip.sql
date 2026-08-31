-- One row per clip, with the event that caused it.
--
-- The mirror of `fct_camera_event`, and it exists because the two questions are
-- different. "What did the cameras see" is answered by the event; "what is on disk,
-- how big is it, and is it still there" is answered by the clip -- and [count withheld] clips
-- had no model at all, so the second question had no table.
--
-- Not every clip has an event: a manual recording has none, and neither does one
-- whose event was dismissed and swept. That is why this is its own fact and not a
-- column on the other one.

with clips as (select * from {{ ref('stg_camera_clips') }}),

events as (select * from {{ ref('int_camera_events') }})

select
    clips.clip_id,
    clips.partition_date,
    clips.camera_id,
    clips.clip_kind,
    clips.clip_title,
    clips.created_at,
    clips.saved_at,
    clips.save_latency_seconds,
    date_trunc('hour', clips.created_at)                as created_hour,
    extract('hour' from clips.created_at)               as created_hour_of_day,
    dayname(clips.created_at)                           as created_weekday,
    (dayofweek(clips.created_at) in (0, 6))             as is_weekend,

    clips.duration_seconds,
    clips.size_bytes,
    clips.bytes_per_second,
    clips.segment_count,
    clips.storage_quality,
    clips.is_archived,
    clips.is_deleted,
    clips.ai_source,
    clips.ai_type,
    clips.local_confidence,
    clips.created_by_hash,
    (clips.created_by_hash is not null)                 as was_saved_by_a_person,
    clips.annotation_count,
    (clips.annotation_count > 0)                        as is_annotated,

    events.event_id,
    events.zone_id,
    events.zone_name,
    events.rule_type,
    events.lifecycle                                    as event_lifecycle,
    events.confidence                                   as event_confidence,
    events.zone_state                                   as event_zone_state,
    events.count_total                                  as event_count_total,
    (events.event_id is not null)                       as has_event,

    clips.unmapped_json,
    clips.extracted_at

from clips
left join events on events.clip_id = clips.clip_id
