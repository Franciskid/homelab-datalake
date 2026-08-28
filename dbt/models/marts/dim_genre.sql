-- Genres as their own dimension, keyed by a surrogate rather than the name.
--
-- The name is the natural key here and it is a French display string that Jellyfin
-- can rewrite when metadata is refreshed. A hash of it keeps joins stable across a
-- rename in a way the string itself does not.

with genres as (
    select distinct genre from {{ ref('stg_jellyfin_title_genres') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['genre']) }} as genre_key,
    genre                                             as genre_name
from genres
