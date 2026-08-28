-- The many-to-many between titles and genres, at snapshot grain.
--
-- A bridge rather than columns on the fact table. A title averages three genres and
-- some carry six; genre_1/genre_2/genre_3 would truncate the tail and make
-- "how many action films" a query nobody can write correctly.
--
-- Snapshot-grained on purpose: a title's genres change when metadata is refreshed, and
-- the bridge should say what was true on the day, not only what is true now.

select
    {{ dbt_utils.generate_surrogate_key(['item_id', 'snapshot_date']) }} as title_key,
    {{ dbt_utils.generate_surrogate_key(['genre']) }}                    as genre_key,
    snapshot_date,
    item_id,
    genre
from {{ ref('stg_jellyfin_title_genres') }}
