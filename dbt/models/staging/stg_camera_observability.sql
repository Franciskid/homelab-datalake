-- Spans where a zone could not be read, and why.
--
-- The other half of every gate reading, and the half the warehouse never had. With
-- state events alone there is no way to tell "the gate stayed shut for six hours" from
-- "nobody could see the gate for six hours", and a report drawing the second as the
-- first is confidently wrong rather than merely incomplete.

with source as (select * from {{ source('raw', 'camera_observability') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as span_id,
        "cameraId"                                  as camera_id,
        "zoneId"                                    as zone_id,
        nullif(reason, '')                          as reason,
        nullif(detail, '')                          as detail,
        try_cast("fromAt" as timestamptz)           as from_at,
        try_cast("toAt"   as timestamptz)           as to_at,
        coalesce(ongoing, false)                    as is_ongoing,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    case when to_at is not null and from_at is not null
         then datediff('second', from_at, to_at) end as duration_seconds,
    -- `state_unconfirmed` is the zone's freshness lease expiring, not the picture
    -- going away. Separating them matters: the first is a timing artefact and the
    -- second is a camera that cannot see.
    (reason = 'state_unconfirmed')                   as is_lease_expiry,
    (reason in ('night_blind', 'lost', 'out_of_view', 'visibility_unreadable'))
                                                     as is_vision_loss
from renamed
