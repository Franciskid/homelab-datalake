-- The few TMDB figures Jellyfin has no field for, one row per id.
--
-- Deliberately short. Jellyfin queries TMDB itself when it matches a file, so the
-- rating (`community_rating` *is* TMDB's vote average), the original title, the
-- production country and the release date are already in the library snapshot at
-- 99.8-[rate withheld] coverage. Only these five are genuinely absent, and only these are worth
-- an API key.
--
-- Not a snapshot and not an event stream: a lookup cache of facts that change on
-- TMDB's schedule. `_enriched_at` records when this pipeline last asked.

with source as (select * from {{ source('raw', 'tmdb_titles') }}),

renamed as (
    select
        cast(tmdb_id as varchar)                as tmdb_id,
        _enriched_at                            as enriched_at,
        nullif(original_language, '')           as original_language,

        -- TMDB reports an unknown budget or revenue as 0. The asset already turns
        -- those into nulls, and it matters: a film that earned nothing and a film
        -- nobody recorded earnings for are not the same row, and averaging them
        -- together understates every box-office question asked of this table.
        budget_usd,
        revenue_usd,
        case
            when budget_usd is not null and budget_usd > 0 and revenue_usd is not null
            then revenue_usd::double / budget_usd
        end                                     as revenue_to_budget,

        popularity,
        -- The count, not the average. The average is already in the library as
        -- community_rating; the count is what says whether to trust it.
        vote_count,
        vote_average,
        runtime_minutes                         as tmdb_runtime_minutes,
        nullif(status, '')                      as release_status,
        nullif(tagline, '')                     as tmdb_tagline,
        nullif(overview, '')                    as tmdb_overview,
        nullif(homepage, '')                    as homepage,
        nullif(release_date, '')                as tmdb_release_date,
        nullif(imdb_id, '')                     as tmdb_imdb_id,
        nullif(original_title, '')              as tmdb_original_title,
        nullif(title, '')                       as tmdb_title,
        is_adult,
        collection_id,
        nullif(collection_name, '')             as collection_name,
        genres                                  as tmdb_genres,
        production_companies,
        production_countries                    as tmdb_production_countries,
        spoken_languages,
        origin_countries
    from source
)

select * from renamed
