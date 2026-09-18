-- One row per result a logged search returned, carrying enough of its parent
-- search -- searched_at, user_id, user_name, search_term and kind -- to be usable
-- on its own. The same reasoning fct_jellyfin_activity's own user_name join
-- documents: a caller asking "what did people click through to" should not have
-- to join back to fct_jellyfin_search first just to know who searched for what.

with results as (

    select * from {{ ref('stg_jellyfin_search_results') }}

),

searches as (

    select search_id, user_id, user_name, search_term, kind
    from {{ ref('stg_jellyfin_searches') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['results.search_id', 'results.position']) }}
                                                         as search_result_key,
    results.partition_date,
    results.partition_hour,
    results.extracted_at,
    results.search_id,
    results.searched_at,
    results.position,
    results.item_id,
    results.name,
    results.type,
    results.year,
    results.series_name,
    results.season_number,
    results.episode_number,
    results.semantic,
    results.semantic_score,
    results.semantic_rank,
    results.reason,
    searches.user_id,
    searches.user_name,
    searches.search_term,
    searches.kind
from results
left join searches on results.search_id = searches.search_id
