-- One row per title per snapshot date: the grain the star schema hangs off.

select
    {{ dbt_utils.generate_surrogate_key(['item_id', 'snapshot_date']) }} as title_key,
    item_id,
    snapshot_date,

    title,
    -- The title as released. Differs from `title` wherever Jellyfin shows a localised
    -- name, which in this library is most of it.
    original_title,
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
    -- From Jellyfin's ProductionLocations, not from TMDB: it is already there, at
    -- [rate withheld] coverage, having been fetched from TMDB during the scan.
    production_country,

    titles.tmdb_id,
    imdb_id,
    -- Enrichment is a key lookup only while the key is present. Naming the gap as a
    -- column means a query can exclude it rather than silently averaging over titles
    -- that were never enriched.
    (titles.tmdb_id is not null) as is_enrichable,

    -- The figures Jellyfin has no field for, joined on the id Jellyfin already
    -- stored -- never on the title, so a film cannot pick up another film's box
    -- office. Null when no API key is configured, which is a supported state: the
    -- rating, original title, country and dates above all come from Jellyfin and are
    -- populated either way.
    tmdb.original_language,
    tmdb.budget_usd,
    tmdb.revenue_usd,
    tmdb.revenue_to_budget,
    tmdb.popularity                             as tmdb_popularity,
    tmdb.vote_count                             as tmdb_vote_count,
    (tmdb.tmdb_id is not null)                  as is_enriched

from {{ ref('stg_jellyfin_titles') }} titles
-- Left, always: a missing enrichment must not remove a film from the library.
left join {{ ref('stg_tmdb_titles') }} tmdb
    on titles.tmdb_id = tmdb.tmdb_id
