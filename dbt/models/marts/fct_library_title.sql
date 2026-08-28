-- One row per title per snapshot date: the grain the star schema hangs off.

select
    {{ dbt_utils.generate_surrogate_key(['item_id', 'snapshot_date']) }} as title_key,
    item_id,
    snapshot_date,

    title,
    production_year,
    decade,
    premiere_date,
    added_at,

    community_rating,
    critic_rating,
    official_rating,

    runtime_minutes,
    is_long_feature,
    is_runtime_plausible,
    container,
    video_codec,
    video_height,
    video_width,
    resolution_class,
    is_high_bit_depth,
    audio_codec,
    audio_channels,
    studio,

    tmdb_id,
    imdb_id,
    -- Enrichment is a key lookup only while the key is present. Naming the gap as a
    -- column means a query can exclude it rather than silently averaging over titles
    -- that were never enriched.
    (tmdb_id is not null) as is_enrichable

from {{ ref('stg_jellyfin_titles') }}
