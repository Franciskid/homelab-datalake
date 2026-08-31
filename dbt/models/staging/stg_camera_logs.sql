-- Camera activity log entries, typed.
--
-- Comments, manual recordings and detection pauses -- the record of what a *person*
-- did, as opposed to what the cameras saw. [count withheld] entries have been landing in the raw
-- layer with no model reading them.
--
-- The free-text title is not exported by the camera and so is not here: on this
-- deployment people write @mentions and asides in it. The type, the target and the
-- timing are what analysis asks about.

with source as (select * from {{ source('raw', 'camera_logs') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as log_id,
        "cameraId"                                  as camera_id,
        nullif(type, '')                            as entry_type,
        coalesce(important, false)                  as is_important,
        try_cast("createdAt" as timestamptz)        as created_at,
        -- The clip or event this entry was written against, when it was written
        -- against one. This is what makes the log joinable rather than a flat feed.
        nullif("sourceId", '')                      as source_id,
        -- Free text, @mentions and all. The household's own notes.
        nullif(title, '')                           as entry_title,
        nullif("userHash", '')                      as user_hash,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    (user_hash is not null)                         as was_written_by_a_person
from renamed
