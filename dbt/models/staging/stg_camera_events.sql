with source as (select * from {{ source('raw', 'camera_events') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as event_id,
        "cameraId"                                  as camera_id,
        "zoneId"                                    as zone_id,
        nullif("zoneName", '')                      as zone_name,
        nullif("ruleType", '')                      as rule_type,
        nullif(lifecycle, '')                       as lifecycle,
        -- The export sends ISO-8601 strings; cast once, here, so nothing downstream
        -- has to guess the format or the zone.
        try_cast("startedAt"       as timestamptz)  as started_at,
        try_cast("completedAt"     as timestamptz)  as completed_at,
        try_cast("stateObservedAt" as timestamptz)  as state_observed_at,
        nullif("clipId", '')                        as clip_id,
        nullif(direction, '')                       as direction,
        nullif(state, '')                           as zone_state,
        nullif("previousState", '')                 as previous_zone_state,
        -- Null is a real value and is not a low score: it means the model declined to
        -- commit. Coalescing it to 0 would turn a refusal into a confident "no".
        confidence,
        coalesce(unusual, false)                    as is_unusual,
        coalesce("shouldNotify", false)             as should_notify,
        nullif("dismissalReason", '')               as dismissal_reason,
        nullif("duplicateOf", '')                   as duplicate_of,
        nullif(summary, '')                         as summary
    from source
)

select
    *,
    (lifecycle = 'confirmed')                       as is_confirmed,
    (lifecycle = 'candidate')                       as is_awaiting_analysis,
    case when completed_at is not null and started_at is not null
         then datediff('second', started_at, completed_at) end as duration_seconds
from renamed
