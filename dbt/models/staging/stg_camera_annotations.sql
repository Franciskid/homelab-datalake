-- Notes and marks left against a clip or an event.
--
-- The note itself never travels -- people write asides and @mentions in it -- so what
-- is here is that a note exists, on what, when, and by which pseudonymised person.
-- That is enough to ask which clips drew attention and which zones people keep
-- returning to, which nothing else in the warehouse can answer.

with source as (select * from {{ source('raw', 'camera_annotations') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as annotation_id,
        nullif(scope, '')                           as scope,
        nullif("targetId", '')                      as target_id,
        nullif(kind, '')                            as annotation_kind,
        nullif(emoji, '')                           as emoji,
        "offsetSec"                                 as offset_seconds,
        "timestampMs"                               as timestamp_ms,
        try_cast("createdAt" as timestamptz)        as created_at,
        nullif(text, '')                            as note_text,
        nullif("userHash", '')                      as user_hash,
        nullif("zoneId", '')                        as zone_id,
        nullif("ruleId", '')                        as rule_id,
        nullif("eventType", '')                     as event_type,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    -- `scope` says which table `target_id` points at, and a reader joining without
    -- checking it would match a clip id against a camera id.
    (scope = 'clip')                                as targets_a_clip,
    (scope = 'live')                                as targets_a_camera,
    (user_hash is not null)                         as was_written_by_a_person
from renamed
