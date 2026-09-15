-- One row per playback session, joined to the title dimension where it resolves.
--
-- **The episode-to-series join, and why there isn't a real one.** dim_title_scd2
-- only ever holds movies: the library snapshot requests `IncludeItemTypes=Movie` and
-- always has -- series and episodes were never fetched as library items, so there is
-- no series-grain row anywhere in the warehouse for an episode's real SeriesId to
-- join against, even if PlaybackActivity exposed that id, which it does not. So the
-- join below is a plain `item_id = item_id`: it resolves for a Movie, because the id
-- Jellyfin issued to the film is the same id the library snapshot used, and it never
-- resolves for an Episode. Rather than leave every television session with no title
-- information at all, `series_name` is parsed at ingest from item_name's own display
-- convention -- see `parse_series_name` in `parsing/jellyfin.py` -- which needs no
-- join and no extra API call, and was verified against five real shows on this
-- server before being written.
--
-- **Genre without fan-out.** bridge_title_genre is many rows per title, and joining
-- it onto a fact table at fact grain would multiply every watch-time measure by the
-- number of genres a film carries. The primary genre is used instead -- the same
-- trade-off `stg_jellyfin_titles` already makes for `studio` and
-- `production_country`, documented there as "the primary one stays, for every
-- caller that only wants one".

with playback as (

    select * from {{ ref('int_jellyfin_playback') }}

),

primary_genre as (

    select item_id, min(genre) as genre
    from {{ ref('bridge_title_genre') }}
    group by item_id

),

library as (

    select item_id, community_rating
    from {{ ref('dim_title_scd2') }}
    where is_current

)

select
    {{ dbt_utils.generate_surrogate_key(
        ['playback.started_at', 'playback.user_id', 'playback.item_id']
    ) }}                                                 as playback_key,
    playback.partition_date,
    playback.partition_hour,
    playback.extracted_at,
    playback.started_at,
    date_trunc('hour', playback.started_at)              as started_hour,
    extract('hour' from playback.started_at)             as started_hour_of_day,
    dayname(playback.started_at)                         as started_weekday,
    (dayofweek(playback.started_at) in (0, 6))           as is_weekend,
    playback.user_id,
    playback.user_name,
    playback.item_id,
    playback.item_type,
    playback.item_name                                   as title,
    playback.series_name,
    playback.is_episode,
    primary_genre.genre,
    playback.playback_method,
    playback.client_name,
    playback.device_name,
    playback.play_duration_seconds,
    (library.item_id is not null)                        as is_in_library,
    library.community_rating                             as library_community_rating
from playback
left join primary_genre on playback.item_id = primary_genre.item_id
left join library on playback.item_id = library.item_id
