-- Server activity log entries, typed. One row per sign-in, playback start or stop,
-- and library or account change -- whatever Jellyfin itself decided to log.

with source as (select * from {{ source('raw', 'jellyfin_activity') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                                as extracted_at,
        id                                            as activity_id,
        date                                          as happened_at,
        nullif(type, '')                              as activity_type,
        nullif(name, '')                               as name,
        nullif(overview, '')                           as overview,
        nullif(short_overview, '')                     as short_overview,
        nullif(user_id, '')                            as user_id,
        nullif(item_id, '')                            as item_id,
        nullif(severity, '')                           as severity,
        nullif(_unmapped, '')                           as unmapped_json
    from source
)

select
    *,
    date_trunc('hour', happened_at)                    as happened_hour,
    (severity = 'Error')                                as is_error,
    (severity = 'Warning')                              as is_warning
from renamed
