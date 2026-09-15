-- Series, seasons, episodes, box sets and other videos, typed. Everything in the
-- library that stg_jellyfin_titles does not cover -- that one is movies only, and
-- stays that way; see fct_library_item.sql for why these are not the same table.

with source as (select * from {{ source('raw', 'jellyfin_library_items') }}),

renamed as (
    select
        dt                                          as snapshot_date,
        item_id,
        nullif(item_type, '')                       as item_type,
        title,
        nullif(original_title, '')                  as original_title,
        production_year,
        premiere_date,
        end_date,
        nullif(status, '')                          as status,
        added_at,
        community_rating,
        critic_rating,
        nullif(official_rating, '')                 as official_rating,
        runtime_minutes,
        nullif(container, '')                       as container,
        nullif(video_codec, '')                      as video_codec,
        video_height,
        video_width,
        nullif(resolution_class, '')                 as resolution_class,
        nullif(audio_codec, '')                      as audio_codec,
        audio_channels,
        nullif(tmdb_id, '')                          as tmdb_id,
        nullif(imdb_id, '')                          as imdb_id,
        nullif(tvdb_id, '')                          as tvdb_id,
        nullif(studio, '')                           as studio,
        studios,
        nullif(production_country, '')               as production_country,
        production_countries,
        genres,
        nullif(sort_name, '')                        as sort_name,
        nullif(overview, '')                         as overview,
        nullif(tagline, '')                          as tagline,
        taglines,
        tags,
        nullif(parent_id, '')                        as parent_id,
        nullif(series_id, '')                        as series_id,
        nullif(series_name, '')                      as series_name,
        nullif(season_id, '')                        as season_id,
        nullif(season_name, '')                      as season_name,
        index_number,
        parent_index_number,
        stream_count,
        remote_trailer_count,
        external_urls,
        file_size_bytes,
        file_bitrate,
        media_source_count,
        nullif(_unmapped, '')                        as unmapped_json
    from source
)

select
    *,
    -- Raw index_number/parent_index_number mean different things depending on
    -- item_type -- a Season's own number on a Season row, an episode's number on an
    -- Episode row, with the season it belongs to in parent_index_number. Resolved
    -- once, here, so nothing downstream has to know that to ask "what season is
    -- this episode in".
    case when item_type = 'Season' then index_number
         when item_type = 'Episode' then parent_index_number end as season_number,
    case when item_type = 'Episode' then index_number end          as episode_number,
    (item_type = 'Episode')                                        as is_episode,
    (item_type = 'Series')                                         as is_series
from renamed
