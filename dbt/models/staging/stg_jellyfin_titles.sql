with source as (select * from {{ source('raw', 'jellyfin_titles') }}),

renamed as (
    select
        dt                                      as snapshot_date,
        item_id,
        title,
        nullif(original_title, '')              as original_title,
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
        nullif(production_country, '')          as production_country,
        decade,
        tmdb_collection_id,
        tvdb_id,
        studios,
        production_countries,
        nullif(sort_name, '')                   as sort_name,
        nullif(overview, '')                    as overview,
        nullif(tagline, '')                     as tagline,
        taglines,
        tags,
        nullif(parent_id, '')                   as parent_id,
        nullif(item_type, '')                   as item_type,
        nullif(media_type, '')                  as media_type,
        nullif(video_type, '')                  as video_type,
        has_subtitles,
        subtitle_languages,
        audio_languages,
        stream_count,
        chapter_count,
        remote_trailer_count,
        external_urls,
        file_size_bytes,
        file_bitrate,
        media_source_count,
        nullif(_unmapped, '')                   as unmapped_json
    from source
)

select
    *,
    -- HDR is bit depth, not codec: hevc is routinely 8-bit, and h264 10-bit exists.
    (video_bit_depth >= 10)                     as is_high_bit_depth,
    (runtime_minutes >= 150)                    as is_long_feature,

    -- Some mpeg2video DVD rips report a duration an order of magnitude too long --
    -- two titles here claim [count withheld] and [count withheld] minutes for films of about 110 and 87.
    -- The container metadata is wrong, not the pipeline.
    --
    -- The value is kept exactly as reported rather than corrected: inventing a
    -- plausible runtime would put a fabricated number in the warehouse and make the
    -- problem invisible. Flagging it instead means every aggregate can exclude these
    -- deliberately, and the count of them is itself a metric worth watching.
    coalesce(runtime_minutes between 1 and 600, false) as is_runtime_plausible
from renamed
