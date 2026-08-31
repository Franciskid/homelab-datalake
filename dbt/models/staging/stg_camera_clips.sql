-- Clip metadata, typed.
--
-- This model did not exist. Clips have been landing in the raw layer since the
-- pipeline was written -- [count withheld] of them -- fully declared and fully documented, and
-- then nothing read them: there was no staging model and no mart, so an event could
-- not reach the clip that filmed it. The join is clean where it is used
-- (`fct_camera_event.clip_id` resolved [count withheld] of [count withheld] distinct ids), it was simply
-- never modelled.

with source as (select * from {{ source('raw', 'camera_clips') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as clip_id,
        "cameraId"                                  as camera_id,
        nullif(kind, '')                            as clip_kind,
        -- What the clip was labelled -- "1 personne". Generated from what the analysis
        -- found, not prose anybody typed, and it is the caption the app itself shows.
        nullif(title, '')                           as clip_title,
        try_cast("createdAt" as timestamptz)        as created_at,
        try_cast("savedAt"   as timestamptz)        as saved_at,
        "durationSeconds"                           as duration_seconds,
        "sizeBytes"                                 as size_bytes,
        "segmentCount"                              as segment_count,
        nullif("storageQuality", '')                as storage_quality,
        coalesce(archived, false)                   as is_archived,
        coalesce(deleted, false)                    as is_deleted,
        nullif("aiSource", '')                      as ai_source,
        nullif("aiType", '')                        as ai_type,
        -- The local detector's own score, which is not the vision model's confidence
        -- on the event. Two different numbers about the same moment; named apart.
        "localConfidence"                           as local_confidence,
        nullif("createdByHash", '')                 as created_by_hash,
        coalesce("annotationCount", 0)              as annotation_count,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    -- How long the clip sat before it was written out. A rising figure here is the
    -- recorder falling behind, which nothing else in the warehouse would show.
    case when saved_at is not null and created_at is not null
         then datediff('second', created_at, saved_at) end as save_latency_seconds,
    case when duration_seconds > 0 and size_bytes > 0
         then size_bytes / duration_seconds end            as bytes_per_second
from renamed
