-- TMDB reference data, one row per id.
--
-- Not a snapshot and not an event stream: a lookup cache of facts that change on
-- TMDB's schedule. `_enriched_at` records when this pipeline last asked, which is what
-- the asset uses to decide what is stale.

with source as (select * from {{ source('raw', 'tmdb_titles') }}),

renamed as (
    select
        cast(tmdb_id as varchar)                as tmdb_id,
        _enriched_at                            as enriched_at,
        nullif(original_language, '')           as original_language,
        nullif(original_title, '')              as original_title,
        try_cast(nullif(release_date, '') as date) as tmdb_release_date,

        -- TMDB reports an unknown budget or revenue as 0. The asset already turns
        -- those into nulls, and this keeps the distinction visible in the model: a
        -- film that earned nothing and a film nobody recorded earnings for are not
        -- the same row, and averaging them together understates every box-office
        -- question asked of this table.
        budget_usd,
        revenue_usd,
        case
            when budget_usd is not null and budget_usd > 0 and revenue_usd is not null
            then revenue_usd::double / budget_usd
        end                                     as revenue_to_budget,

        popularity,
        -- A vote average over zero votes is 0.0, which is not a rating of zero, so
        -- the count travels beside it and the average is nulled when nobody voted.
        case when coalesce(vote_count, 0) > 0 then vote_average end as vote_average,
        vote_count,

        is_adult,
        nullif(status, '')                      as tmdb_status,
        nullif(production_country, '')          as production_country
    from source
)

select * from renamed
