with source as (select * from {{ source('raw', 'jellyfin_titles') }}),

renamed as (
    select
        dt                                      as snapshot_date,
        item_id,
        title,
        production_year,
        premiere_date,
        added_at,
        community_rating,
        critic_rating,
        nullif(official_rating, '')             as official_rating,
        runtime_minutes,
        nullif(container, '')                   as container,
        nullif(video_codec, '')                 as video_codec,
        video_height,
        video_width,
        video_bit_depth,
        nullif(resolution_class, '')            as resolution_class,
        nullif(audio_codec, '')                 as audio_codec,
        audio_channels,
        nullif(tmdb_id, '')                     as tmdb_id,
        nullif(imdb_id, '')                     as imdb_id,
        nullif(studio, '')                      as studio,
        decade
    from source
)

select
    *,
    -- HDR is bit depth, not codec: hevc is routinely 8-bit, and h264 10-bit exists.
    (video_bit_depth >= 10)                     as is_high_bit_depth,
    (runtime_minutes >= 150)                    as is_long_feature
from renamed
