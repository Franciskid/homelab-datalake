-- One row per playback session, joined to the title dimension where it resolves.
--
-- **The episode-to-series join, updated 2026-09-15.** Until WS-41.B's follow-up,
-- dim_title_scd2 only ever held movies -- the library snapshot requested
-- `IncludeItemTypes=Movie` and nothing else -- so there was no series-grain row
-- anywhere to join an episode's SeriesId against, and `series_name` was parsed out of
-- item_name's own display convention instead. `fct_library_item` now snapshots
-- series, seasons and episodes daily, so an episode's `item_id` resolves directly
-- against it: `series_id`, a real `series_name`, `season_number` and `episode_number`
-- all come from the join below rather than a string parse.
--
-- The parse is not deleted -- it is still computed at ingest, in
-- `parse_series_name`, onto every row of `stg_jellyfin_playback` -- and is kept as
-- the documented fallback for exactly the rows the id join cannot reach: an episode
-- played before library-item collection began (2026-09-15), later removed from the
-- library, or re-imported under a different id. Measured on 2026-09-15 against the
-- real playback history: 62 of 988 distinct episodes ever played ([rate withheld]) do not
-- resolve by id, covering 130 of [count withheld] episode playback rows ([rate withheld]) -- the fallback
-- is live on roughly one in fourteen episode rows, not a formality kept for show.
-- `series_resolved_by_id` says, per row, which path supplied `series_name`.
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

),

-- fct_library_item is a plain snapshot, not SCD2 -- see its own model for why -- so
-- "the current answer" means the most recent snapshot per item rather than a flag.
latest_episode as (

    select
        item_id, series_id, series_name, season_number, episode_number,
        row_number() over (partition by item_id order by snapshot_date desc) as recency
    from {{ ref('fct_library_item') }}
    where item_type = 'Episode'

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
    latest_episode.series_id,
    coalesce(latest_episode.series_name, playback.series_name) as series_name,
    (latest_episode.item_id is not null)                 as series_resolved_by_id,
    latest_episode.season_number,
    latest_episode.episode_number,
    playback.is_episode,
    primary_genre.genre,
    playback.playback_method,
    playback.client_name,
    playback.device_name,
    playback.play_duration_seconds,
    (library.item_id is not null)                        as is_in_library,
    library.community_rating                             as library_community_rating
from playback
left join latest_episode
    on playback.item_id = latest_episode.item_id and latest_episode.recency = 1
left join primary_genre on playback.item_id = primary_genre.item_id
left join library on playback.item_id = library.item_id
