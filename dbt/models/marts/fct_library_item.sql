-- One row per series, season, episode, box set or other video per snapshot.
--
-- **Why this is not dim_title_scd2, and why these are not the same table as
-- fct_library_title.** dim_title_scd2 versions five attributes -- title, rating,
-- video codec, resolution, runtime -- on the premise that a movie is one file whose
-- occasional re-encode or rating change is worth keeping history for. None of that
-- premise survives contact with the other five item types: a Season and a BoxSet are
-- containers with no file of their own, a Series' "rating" and "runtime" are borrowed
-- from whichever episode Jellyfin last looked at rather than a fact about the show,
-- and versioning five very different kinds of change (an episode's file being
-- re-encoded, a season being added, a series ending) would mean five different
-- change-detection rules wearing one SCD2 model's clothes. What is actually needed --
-- resolve an episode's series, get a series' status and dates, answer "how many
-- episodes are in the library" -- is answered completely by a snapshot fact refreshed
-- daily, the same grain the raw layer already lands at. So dim_title_scd2 and
-- fct_library_title stay movie-only, unchanged, and this is a second, separate mart
-- at a second, separate grain -- not an extension of the first.
--
-- Genres, tags and studios stay as list columns rather than a bridge and a dimension
-- the way the movie library has them: bridge_title_genre exists because a movie
-- question asks "how many action films", at [count withheld]-title volume. Nothing here asks
-- that question yet, and building a second bridge for [count withheld] items across five grains
-- to answer a question nobody has asked is not what this ingestion needs.

select
    {{ dbt_utils.generate_surrogate_key(['item_id', 'snapshot_date']) }} as item_key,
    item_id,
    snapshot_date,
    item_type,
    is_series,
    is_episode,
    title,
    original_title,
    production_year,
    premiere_date,
    end_date,
    status,
    added_at,
    community_rating,
    critic_rating,
    official_rating,
    runtime_minutes,
    container,
    video_codec,
    video_height,
    video_width,
    resolution_class,
    audio_codec,
    audio_channels,
    tmdb_id,
    imdb_id,
    tvdb_id,
    studio,
    studios,
    production_country,
    production_countries,
    genres,
    sort_name,
    overview,
    tagline,
    taglines,
    tags,
    parent_id,
    series_id,
    series_name,
    season_id,
    season_name,
    season_number,
    episode_number,
    stream_count,
    remote_trailer_count,
    external_urls,
    file_size_bytes,
    file_bitrate,
    media_source_count,
    unmapped_json
from {{ ref('stg_jellyfin_library_items') }}
