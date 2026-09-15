-- Playback sessions, typed.
--
-- `started_at` and `play_duration_seconds` are already correct here, not derived:
-- the raw asset converted the plugin's naive SQLite timestamp to UTC and its
-- PlayDuration to whole seconds at ingest, both verified against real sessions on
-- this server rather than assumed -- see `_parse_playback_timestamp` and
-- `_parse_duration_seconds` in `parsing/jellyfin.py`. Nothing here repeats that work.

with source as (select * from {{ source('raw', 'jellyfin_playback') }}),

renamed as (
    select
        dt                                          as partition_date,
        partition_hour,
        _extracted_at                                as extracted_at,
        started_at,
        nullif(user_id, '')                          as user_id,
        nullif(user_name, '')                         as user_name,
        nullif(item_id, '')                           as item_id,
        nullif(item_type, '')                         as item_type,
        nullif(item_name, '')                         as item_name,
        -- Parsed at ingest from item_name's own "<series> - sNNeNN - <title>"
        -- convention -- null for anything that is not an Episode. See
        -- `parse_series_name` for why this needs no join.
        nullif(series_name, '')                       as series_name,
        nullif(playback_method, '')                   as playback_method,
        nullif(client_name, '')                        as client_name,
        nullif(device_name, '')                        as device_name,
        play_duration_seconds
    from source
)

select
    *,
    (item_type = 'Episode')                          as is_episode
from renamed
