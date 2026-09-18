-- Search results, typed. One row per item a logged search returned, capped at
-- 250 per search by the plugin -- result_count on the parent search is always
-- the true count, results_logged how many of them landed here.

with source as (select * from {{ source('raw', 'jellyfin_search_results') }}),

renamed as (
    select
        dt                                          as partition_date,
        partition_hour,
        _extracted_at                                as extracted_at,
        search_id,
        searched_at,
        position,
        nullif(item_id, '')                           as item_id,
        nullif(name, '')                               as name,
        nullif(type, '')                               as type,
        year,
        nullif(series_name, '')                        as series_name,
        season_number,
        episode_number,
        semantic,
        semantic_score,
        semantic_rank,
        nullif(reason, '')                              as reason
    from source
)

select * from renamed
