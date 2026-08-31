-- One row per camera event, latest version, with the clip that filmed it.
--
-- The grain analysis works at, and deliberately wide: this is the table an export
-- reads, and a column that is here is a column somebody can put in a spreadsheet
-- without writing a join. Three things it gained, each because their absence made a
-- real question unanswerable:
--
--   * **The subject counts.** Every graph of this data is a sum over them. They were
--     dropped at ingest and nothing said so, so the graph could not be rebuilt here.
--   * **The clip.** `clip_id` pointed at a table that had no model, so an event could
--     not reach its own footage. The join is exact -- every clip id resolves.
--   * **The correction.** A figure a person overrode looked identical to one nobody
--     touched, so "how often is the model wrong about the gate" had nothing to read.

with events as (select * from {{ ref('int_camera_events') }}),

clips as (select * from {{ ref('stg_camera_clips') }})

select
    events.event_id,
    events.partition_date,
    events.camera_id,
    events.zone_id,
    events.zone_name,
    events.rule_id,
    events.rule_type,
    events.lifecycle,
    events.is_confirmed,
    events.is_awaiting_analysis,
    events.is_dismissed,

    events.started_at,
    events.occurred_at,
    -- The instant to rank by, resolved in staging: `occurred_at` when the camera knows
    -- it, `started_at` otherwise. Ordering by `started_at` alone disagrees with the
    -- camera's own report on any event a replay or repair has moved.
    events.happened_at,
    events.completed_at,
    -- Restored. It survived raw, staging and intermediate and was dropped here, which
    -- for a gate transition loses the instant the state was actually observed.
    events.state_observed_at,
    events.state_source,
    events.duration_seconds,
    date_trunc('hour', events.happened_at)                  as started_hour,
    -- Hour-of-day and weekday are the analysis on event data: when does the garden get
    -- busy, does the weekend differ. Derived in UTC, pinned by the session setting.
    extract('hour' from events.happened_at)                 as started_hour_of_day,
    dayname(events.happened_at)                             as started_weekday,
    (dayofweek(events.happened_at) in (0, 6))               as is_weekend,

    events.direction,
    events.zone_state,
    events.previous_zone_state,
    (events.zone_state is distinct from events.previous_zone_state) as state_changed,

    events.count_person,
    events.count_vehicle,
    events.count_animal,
    events.count_other,
    events.count_total,

    events.confidence,
    events.is_unusual,
    events.should_notify,
    events.dismissal_reason,
    events.duplicate_of,
    (events.duplicate_of is not null)                       as is_duplicate,
    events.summary,

    -- What a person changed, and what the machine had said. The row above is the
    -- corrected truth; these say so and say who.
    events.was_corrected,
    events.corrected_at,
    events.corrected_by_name,
    events.corrected_by_hash,
    events.machine_lifecycle,
    events.machine_direction,
    events.machine_zone_state,
    events.machine_previous_zone_state,
    events.machine_state_observed_at,
    events.machine_dismissal_reason,
    events.machine_duplicate_of,
    events.machine_summary,
    events.machine_confidence,
    events.machine_should_notify,
    events.machine_count_person,
    events.machine_count_vehicle,
    events.machine_count_animal,
    events.machine_count_other,
    -- The two corrections that actually change a headline figure, called out so a
    -- reader does not have to compare eleven pairs of columns to find them.
    (events.was_corrected and events.machine_lifecycle is distinct from events.lifecycle)
                                                            as lifecycle_was_corrected,
    (events.was_corrected and events.machine_zone_state is distinct from events.zone_state)
                                                            as zone_state_was_corrected,

    events.clip_id,
    -- For a state change `clip_id` shows a gate that has already finished moving;
    -- this is the clip that filmed it moving. Readers wanting the transition want it.
    events.transition_clip_id,
    clips.clip_kind,
    clips.clip_title,
    clips.duration_seconds                                  as clip_duration_seconds,
    clips.size_bytes                                        as clip_size_bytes,
    clips.storage_quality                                   as clip_storage_quality,
    clips.is_archived                                       as clip_is_archived,
    clips.is_deleted                                        as clip_is_deleted,
    clips.ai_source                                         as clip_ai_source,
    clips.ai_type                                           as clip_ai_type,
    clips.local_confidence                                  as clip_local_confidence,
    clips.annotation_count                                  as clip_annotation_count,
    (clips.clip_id is not null)                             as has_clip,

    events.backfill_json,
    events.repair_json,
    events.restoration_json,
    events.timing_correction_json,
    -- Non-null means the camera sent a field the schema does not name. Kept on the
    -- fact rather than only in raw, because the person who notices it is the one
    -- looking at the data, not the one reading asset checks.
    events.unmapped_json,
    events.extracted_at

from events
-- Left, not inner: an event whose analysis has not produced a clip yet is still an
-- event, and an inner join would silently drop it from every count.
left join clips on clips.clip_id = events.clip_id
