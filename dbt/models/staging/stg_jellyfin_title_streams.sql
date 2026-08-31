-- Media streams, typed. One row per audio, video or subtitle track.
--
-- The table that did not exist. `stg_jellyfin_titles` keeps the first video stream and
-- the first audio stream, which is enough to say "this is a 1080p H264 file" and not
-- enough to say anything else: every subtitle track in the library and every audio
-- track after the first were discarded at ingest, so a film with French and English
-- audio and thirty-four subtitle tracks was recorded as having one of each.

with source as (select * from {{ source('raw', 'jellyfin_title_streams') }}),

renamed as (
    select
        dt                                          as snapshot_date,
        item_id,
        stream_index,
        nullif(stream_type, '')                     as stream_type,
        nullif(codec, '')                           as codec,
        nullif(language, '')                        as language,
        nullif(title, '')                           as stream_title,
        nullif(display_title, '')                   as display_title,
        is_default,
        is_forced,
        is_external,
        is_hearing_impaired,
        is_interlaced,
        nullif(height, 0)                           as height,
        nullif(width, 0)                            as width,
        channels,
        nullif(channel_layout, '')                  as channel_layout,
        bit_rate,
        bit_depth,
        sample_rate,
        nullif(video_range, '')                     as video_range,
        nullif(video_range_type, '')                as video_range_type,
        nullif(profile, '')                         as profile,
        level
    from source
)

select
    *,
    -- The natural key. `stream_index` is unique only within a title.
    item_id || ':' || cast(stream_index as varchar)  as stream_key,
    (stream_type = 'Video')                          as is_video,
    (stream_type = 'Audio')                          as is_audio,
    (stream_type = 'Subtitle')                       as is_subtitle,
    -- 10-bit and an HDR range together are what actually make a file HDR; either
    -- alone is a claim the other does not support.
    (video_range is not null and video_range <> 'SDR') as is_hdr
from renamed
